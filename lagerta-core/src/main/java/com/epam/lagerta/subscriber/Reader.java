/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lagerta.subscriber;

import com.epam.lagerta.capturer.TransactionScope;
import com.epam.lagerta.common.Scheduler;
import com.epam.lagerta.kafka.DataRecoveryConfig;
import com.epam.lagerta.kafka.KafkaFactory;
import com.epam.lagerta.kafka.SubscriberConfig;
import com.epam.lagerta.services.LeadService;
import com.epam.lagerta.util.Serializer;
import org.apache.ignite.Ignite;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;

public class Reader extends Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Reader.class);

    private static final int POLL_TIMEOUT = 200;
    private static final int DEFAULT_COMMIT_ITERATION_PERIOD = 5;
    private static final long DEFAULT_BUFFER_CLEAR_PERIOD = TimeUnit.SECONDS.toMillis(10L);
    private static final long DEFAULT_BUFFER_CHECK_PERIOD = TimeUnit.SECONDS.toMillis(10L);

    private static final Comparator<TransactionScope> SCOPE_COMPARATOR = comparingLong(TransactionScope::getTransactionId);
    private static final Function<TopicPartition, CommittedOffset> COMMITTED_OFFSET = key -> new CommittedOffset();

    private final KafkaFactory kafkaFactory;
    private final LeadService lead;
    private final SubscriberConfig config;
    private final String reconciliationTopic;
    private final Serializer serializer;
    private final CommitStrategy commitStrategy;
    private final UUID readerId;
    private final BooleanSupplier needToCommitToKafka;
    private final long bufferClearPeriod;
    private final Predicate<Map<Long, TransactionData>> bufferOverflowCondition;
    private final long bufferCheckPeriod;

    private final Map<Long, TransactionData> buffer = new HashMap<>();
    private final Map<TopicPartition, CommittedOffset> committedOffsetMap = new HashMap<>();
    private final AtomicBoolean suspended = new AtomicBoolean(false);

    public Reader(Ignite ignite, KafkaFactory kafkaFactory, SubscriberConfig config,
                  DataRecoveryConfig dataRecoveryConfig, Serializer serializer, CommitStrategy commitStrategy,
                  UUID readerId, Predicate<Map<Long, TransactionData>> bufferOverflowCondition) {
        this(ignite, kafkaFactory, config, dataRecoveryConfig, serializer, commitStrategy,
                new PeriodicIterationCondition(DEFAULT_COMMIT_ITERATION_PERIOD), DEFAULT_BUFFER_CLEAR_PERIOD,
                readerId, bufferOverflowCondition, DEFAULT_BUFFER_CHECK_PERIOD);
    }

    public Reader(Ignite ignite, KafkaFactory kafkaFactory, SubscriberConfig config,
                  DataRecoveryConfig dataRecoveryConfig, Serializer serializer, CommitStrategy commitStrategy,
                  BooleanSupplier needToCommitToKafka, long bufferClearPeriod, UUID readerId,
                  Predicate<Map<Long, TransactionData>> bufferOverflowCondition, long bufferCheckPeriod) {
        this.kafkaFactory = kafkaFactory;
        lead = ignite.services().serviceProxy(LeadService.NAME, LeadService.class, false);
        this.config = config;
        this.reconciliationTopic = dataRecoveryConfig.getReconciliationTopic();
        this.serializer = serializer;
        this.commitStrategy = commitStrategy;
        this.readerId = readerId;
        this.needToCommitToKafka = needToCommitToKafka;
        this.bufferClearPeriod = bufferClearPeriod;
        this.bufferOverflowCondition = bufferOverflowCondition;
        this.bufferCheckPeriod = bufferCheckPeriod;
    }

    @Override
    public void execute() {
        try (ConsumerReader consumer = new ConsumerReader()) {
            registerRule(consumer::pollAndCommitTransactionsBatch);
            when(needToCommitToKafka).execute(consumer::commitOffsets);
            per(bufferClearPeriod).execute(this::clearBuffer);
            per(bufferCheckPeriod).execute(consumer::checkBufferCondition);
            super.execute();
        }
    }

    public void resendReadTransactions() {
        pushTask(() -> {
            List<TransactionScope> collect = buffer.values().stream()
                    .map(TransactionData::getTransactionScope)
                    .collect(Collectors.toList());
            approveAndCommitTransactionsBatch(collect);
        });
    }

    public boolean isSuspended() {
        return suspended.get();
    }

    private void approveAndCommitTransactionsBatch(List<TransactionScope> scopes) {
        List<Long> txIdsToCommit = lead.notifyRead(readerId, scopes);

        if (!txIdsToCommit.isEmpty()) {
            txIdsToCommit.sort(Long::compareTo);
            LOGGER.trace("[R] {} told to commit {}", readerId, txIdsToCommit);

            List<Long> committed = commitStrategy.commit(txIdsToCommit, buffer);
            lead.notifyCommitted(readerId, committed);
            removeFromBufferAndCallNotifyCommit(committed);
        }
    }

    private void clearBuffer() {
        long lastDenseCommittedTxId = lead.getLastDenseCommitted();
        List<Long> committed = buffer.keySet()
                .stream()
                .filter(txID -> txID <= lastDenseCommittedTxId)
                .collect(Collectors.toList());
        removeFromBufferAndCallNotifyCommit(committed);
    }

    private void removeFromBufferAndCallNotifyCommit(List<Long> txIDs) {
        txIDs.stream()
                .map(buffer::remove)
                .forEach(transactionData -> {
                    CommittedOffset offset = committedOffsetMap.get(transactionData.getTopicPartition());
                    if (offset != null) {
                        offset.notifyCommit(transactionData.getOffset());
                    }
                });
    }

    class ConsumerReader implements AutoCloseable {
        private final Consumer<ByteBuffer, ByteBuffer> consumer;

        ConsumerReader() {
            consumer = kafkaFactory.consumer(config.getConsumerConfig());
            consumer.subscribe(Arrays.asList(config.getInputTopic(), reconciliationTopic),
                    new ReaderRebalanceListener(this, committedOffsetMap));
        }

        private void pollAndCommitTransactionsBatch() {
            ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(POLL_TIMEOUT);
            List<TransactionScope> scopes = new ArrayList<>(records.count());
            for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
                TransactionScope transactionScope = serializer.deserialize(record.key());
                if (transactionScope.getScope().isEmpty()) {
                    LOGGER.warn("[R] {} polled empty transaction {}", readerId, transactionScope.getTransactionId());
                }
                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                buffer.put(transactionScope.getTransactionId(),
                        new TransactionData(transactionScope, record.value(), topicPartition, record.offset()));
                scopes.add(transactionScope);
                committedOffsetMap.computeIfAbsent(topicPartition, COMMITTED_OFFSET).notifyRead(record.offset());
            }
            if (!scopes.isEmpty()) {
                scopes.sort(SCOPE_COMPARATOR);
                LOGGER.trace("[R] {} polled {}", readerId, scopes);
            }
            approveAndCommitTransactionsBatch(scopes);
        }

        void commitOffsets() {
            Map<TopicPartition, OffsetAndMetadata> offsets = committedOffsetMap.entrySet().stream()
                    .peek(entry -> entry.getValue().compress())
                    .filter(entry -> entry.getValue().getLastDenseCommit() >= 0)
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> new OffsetAndMetadata(entry.getValue().getLastDenseCommit()))
                    );
            consumer.commitSync(offsets);
        }

        private void checkBufferCondition() {
            if (bufferOverflowCondition.test(buffer)) {
                if (suspended.compareAndSet(false, true)) {
                    consumer.pause(getMainTopicPartitions());
                }
            } else {
                if (suspended.compareAndSet(true, false)) {
                    consumer.resume(getMainTopicPartitions());
                }
            }
        }

        private List<TopicPartition> getMainTopicPartitions() {
            String inputTopic = config.getInputTopic();
            return consumer.assignment().stream()
                    .filter(topicPartition -> inputTopic.equals(topicPartition.topic()))
                    .collect(Collectors.toList());
        }

        @Override
        public void close() {
            consumer.close();
        }
    }
}
