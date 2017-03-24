/*
 * Copyright (c) 2017. EPAM Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.activestore.impl.subscriber.consumer;

import org.apache.ignite.activestore.commons.serializer.Serializer;
import org.apache.ignite.activestore.commons.tasks.ExtendedScheduler;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadResponse;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadService;
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.activestore.subscriber.Committer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.ignite.activestore.commons.UUIDFormat.f;

/**
 * @author Aleksandr_Meterko
 * @since 12/15/2016
 */
public class SubscriberConsumer extends ExtendedScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriberConsumerService.class);
    private static final int POLL_TIMEOUT = 200;
    private static final int POLL_CYCLES_BETWEEN_COMPACTION = 10;
    private static final long COMMIT_OFFSETS_PERIOD = 1_000;

    private static final Comparator<TransactionMetadata> COMPARATOR = new Comparator<TransactionMetadata>() {
        @Override
        public int compare(TransactionMetadata o1, TransactionMetadata o2) {
            return Long.compare(o1.getTransactionId(), o2.getTransactionId());
        }
    };

    private final LeadService lead;
    private final Serializer serializer;
    private final Committer committer;
    private final TransactionsBuffer buffer;
    private final DoneNotifier doneNotifier;
    private final FullCommitHandler fullCommitHandler;
    private final DeserializerClosure deserializerClosure;
    private final BufferOverflowCondition bufferOverflowCondition;
    private final Consumer<ByteBuffer, ByteBuffer> consumer;

    private final UUID consumerId;
    private final String remoteTopic;
    private final String reconciliationTopic;

    private volatile boolean metadataResendRequested = false;
    private volatile boolean paused = false;

    @Inject
    public SubscriberConsumer(
        LeadService lead,
        Serializer serializer,
        Committer committer,
        TransactionsBuffer buffer,
        DoneNotifier doneNotifier,
        FullCommitHandler fullCommitHandler,
        DeserializerClosure deserializerClosure,
        BufferOverflowCondition bufferOverflowCondition,
        KafkaFactory kafkaFactory,
        DataRecoveryConfig dataRecoveryConfig,
        @Named(DataCapturerBusConfiguration.NODE_ID) UUID consumerId,
        OnKafkaStop onKafkaStop
    ) {
        this.lead = lead;
        this.serializer = serializer;
        this.committer = committer;
        this.buffer = buffer;
        this.doneNotifier = doneNotifier.setBuffer(buffer);
        this.fullCommitHandler = fullCommitHandler.setBuffer(buffer);
        this.deserializerClosure = deserializerClosure.setBuffer(buffer);
        this.bufferOverflowCondition = bufferOverflowCondition;
        consumer = kafkaFactory.consumer(dataRecoveryConfig.getConsumerConfig(), onKafkaStop);

        this.consumerId = consumerId;
        remoteTopic = dataRecoveryConfig.getRemoteTopic();
        reconciliationTopic = dataRecoveryConfig.getReconciliationTopic();
    }

    @Override
    public void execute() {
        simpleTask(new Runnable() {
            @Override
            public void run() {
                pollTransaction();
            }
        });
        OffsetCalculator offsetCalculator = new OffsetCalculator();
        schedulePeriodicTask(new CommitOffsetsPeriodicTask(buffer, offsetCalculator, consumer),
            COMMIT_OFFSETS_PERIOD);
        schedulePeriodicTask(new BufferOverflowConditionPeriodicTask(this, buffer,
                bufferOverflowCondition, lead), COMMIT_OFFSETS_PERIOD);
        schedulePeriodicTask(new Runnable() {
            @Override
            public void run() {
                buffer.compact(lead.getLastDenseCommittedId());
            }
        }, POLL_CYCLES_BETWEEN_COMPACTION);

        LOGGER.info("[C] Started polling kafka for messages");
        consumer.subscribe(Arrays.asList(remoteTopic, reconciliationTopic),
            new RebalanceListener(buffer, offsetCalculator, consumer));
        try {
            super.execute();
        }
        finally {
            LOGGER.info("[C] Ended polling kafka for messages");
            consumer.close();
            doneNotifier.close();
        }
    }

    private void pollTransaction() {
        ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(POLL_TIMEOUT);
        List<TransactionMetadata> metadatas;

        if (metadataResendRequested) {
            metadataResendRequested = false;
            metadatas = new ArrayList<>(buffer.size() + records.count());
            for (TransactionWrapper wrapper : buffer.getUncommittedTxs()) {
                metadatas.add(wrapper.deserializedMetadata());
            }
            LOGGER.info("[C] Resending metadata {} from {}", metadatas.size(), f(consumerId));
        }
        else {
            metadatas = new ArrayList<>(records.count());
        }
        for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
            TransactionMetadata metadata = serializer.deserialize(record.key());
            TransactionWrapper wrapper = new TransactionWrapper(record, metadata);

            metadatas.add(metadata);
            buffer.add(wrapper);
        }
        Collections.sort(metadatas, COMPARATOR);
        if (!metadatas.isEmpty()) {
            LOGGER.debug("[C] Before send {} from {}", metadatas, f(consumerId));
        }
        LeadResponse response = lead.notifyTransactionsRead(consumerId, metadatas);
        if (response != null) {
            if (response.getAlreadyProcessedIds() != null) {
                buffer.markAlreadyCommitted(response.getAlreadyProcessedIds());
                LOGGER.debug("[C] Remove {} in {}", response.getAlreadyProcessedIds(), f(consumerId));
            }
            if (response.getToCommitIds() != null) {
                LOGGER.debug("[C] Gotcha {} in {}", response.getToCommitIds(), f(consumerId));
                buffer.markInProgress(response.getToCommitIds());
                committer.commitAsync(response.getToCommitIds(), deserializerClosure, doneNotifier, fullCommitHandler);
            }
        }
    }

    public void resendTransactionsMetadata() {
        metadataResendRequested = true;
    }

    public boolean isPaused() {
        return paused;
    }

    public void pause() {
        LOGGER.info("[C] {} is paused", f(consumerId));
        paused = true;
        consumer.pause(getRemoteTopicPartitions());
    }

    public void resume() {
        if (paused) {
            enqueueTask(new Runnable() {
                @Override
                public void run() {
                    paused = false;
                    consumer.resume(getRemoteTopicPartitions());
                    LOGGER.info("[C] {} is resumed", f(consumerId));
                }
            });
        }
    }

    private List<TopicPartition> getRemoteTopicPartitions() {
        List<TopicPartition> partitions = new ArrayList<>();
        for (TopicPartition topicPartition : consumer.assignment()) {
            if (remoteTopic.equals(topicPartition.topic())) {
                partitions.add(topicPartition);
            }
        }
        return partitions;
    }
}
