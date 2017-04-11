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
package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.common.PeriodicRule;
import com.epam.lathgertha.common.PredicateRule;
import com.epam.lathgertha.common.Scheduler;
import com.epam.lathgertha.kafka.KafkaFactory;
import com.epam.lathgertha.kafka.SubscriberConfig;
import com.epam.lathgertha.services.LeadService;
import com.epam.lathgertha.util.Serializer;
import org.apache.ignite.Ignite;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Reader extends Scheduler {
    private static final int POLL_TIMEOUT = 200;
    private static final int DEFAULT_COMMIT_ITERATION_PERIOD = 5;
    private static final long DEFAULT_BUFFER_CLEAR_TIME_INTERVAL = TimeUnit.SECONDS.toMillis(10L);
    private static final Comparator<TransactionScope> SCOPE_COMPARATOR = Comparator.comparingLong(TransactionScope::getTransactionId);
    private static final Function<TopicPartition, CommittedOffset> COMMITTED_OFFSET = key -> new CommittedOffset();

    private final KafkaFactory kafkaFactory;
    private final LeadService lead;
    private final SubscriberConfig config;
    private final Serializer serializer;
    private final CommitStrategy commitStrategy;
    private final UUID readerId;
    private final BooleanSupplier commitToKafkaSupplier;
    private final long bufferClearTimeInterval;

    private final Map<Long, TransactionData> buffer = new HashMap<>();
    private final Map<TopicPartition, CommittedOffset> committedOffsetMap = new HashMap<>();

    public Reader(Ignite ignite, KafkaFactory kafkaFactory, SubscriberConfig config, Serializer serializer,
                  CommitStrategy commitStrategy, UUID readerId) {
        this(ignite, kafkaFactory, config, serializer, commitStrategy,
                new PeriodicIterationCondition(DEFAULT_COMMIT_ITERATION_PERIOD), DEFAULT_BUFFER_CLEAR_TIME_INTERVAL,
                readerId);
    }

    public Reader(Ignite ignite, KafkaFactory kafkaFactory, SubscriberConfig config, Serializer serializer,
                  CommitStrategy commitStrategy, BooleanSupplier commitToKafkaSupplier, long bufferClearTimeInterval,
                  UUID readerId) {
        this.kafkaFactory = kafkaFactory;
        lead = ignite.services().serviceProxy(LeadService.NAME, LeadService.class, false);
        this.config = config;
        this.serializer = serializer;
        this.commitStrategy = commitStrategy;
        this.readerId = readerId;
        this.commitToKafkaSupplier = commitToKafkaSupplier;
        this.bufferClearTimeInterval = bufferClearTimeInterval;
    }

    @Override
    public void execute() {
        registerRule(new PeriodicRule(this::clearBuffer, bufferClearTimeInterval));
        try (Consumer<ByteBuffer, ByteBuffer> consumer = createConsumer()) {
            registerRule(() -> pollAndCommitTransactionsBatch(consumer));
            registerRule(new PredicateRule(() -> commitOffsets(consumer, committedOffsetMap), commitToKafkaSupplier));
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

    private Consumer<ByteBuffer, ByteBuffer> createConsumer() {
        Consumer<ByteBuffer, ByteBuffer> consumer = kafkaFactory.consumer(config.getConsumerConfig());
        consumer.subscribe(Collections.singletonList(config.getRemoteTopic()),
                new ReaderRebalanceListener(consumer, committedOffsetMap));
        return consumer;
    }

    private void pollAndCommitTransactionsBatch(Consumer<ByteBuffer, ByteBuffer> consumer) {
        ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(POLL_TIMEOUT);
        List<TransactionScope> scopes = new ArrayList<>(records.count());
        for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
            TransactionScope transactionScope = serializer.deserialize(record.key());
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            buffer.put(transactionScope.getTransactionId(),
                    new TransactionData(transactionScope, record.value(), topicPartition, record.offset()));
            scopes.add(transactionScope);
            committedOffsetMap.computeIfAbsent(topicPartition, COMMITTED_OFFSET).notifyRead(record.offset());
        }
        if (!scopes.isEmpty()) {
            scopes.sort(SCOPE_COMPARATOR);
        }
        approveAndCommitTransactionsBatch(scopes);
    }


    private void approveAndCommitTransactionsBatch(List<TransactionScope> scopes) {
        List<Long> txIdsToCommit = lead.notifyRead(readerId, scopes);

        if (!txIdsToCommit.isEmpty()) {
            txIdsToCommit.sort(Long::compareTo);
            List<Long> committed = commitStrategy.commit(txIdsToCommit, buffer);
            lead.notifyCommitted(committed);
            removeFromBufferAndCallNotifyCommit(committed);
        }
    }

    static void commitOffsets(Consumer consumer, Map<TopicPartition, CommittedOffset> committedOffsetMap) {
        Map<TopicPartition, OffsetAndMetadata> offsets = committedOffsetMap.entrySet().stream()
                .peek(entry -> entry.getValue().compress())
                .filter(entry -> entry.getValue().getLastDenseCommit() >= 0)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new OffsetAndMetadata(entry.getValue().getLastDenseCommit()))
                );
        consumer.commitSync(offsets);
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
}
