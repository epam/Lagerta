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
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

public class Reader extends Scheduler {
    private static final int POLL_TIMEOUT = 200;
    private static final int DEFAULT_COMMIT_ITERATION_PERIOD = 5;

    private final BooleanSupplier commitToKafkaSupplier;
    private final KafkaFactory kafkaFactory;
    private final LeadService lead;
    private final SubscriberConfig config;
    private final Serializer serializer;
    private final CommitStrategy commitStrategy;
    private final UUID nodeId;

    private final Map<Long, TransactionData> buffer = new HashMap<>();
    private final Map<TopicPartition, CommittedOffset> committedOffsetMap = new HashMap<>();

    public Reader(Ignite ignite, KafkaFactory kafkaFactory, SubscriberConfig config, Serializer serializer,
                  CommitStrategy commitStrategy) {
        this(ignite, kafkaFactory, config, serializer, commitStrategy,
                new PeriodicIterationCondition(DEFAULT_COMMIT_ITERATION_PERIOD));
    }

    public Reader(Ignite ignite, KafkaFactory kafkaFactory, SubscriberConfig config, Serializer serializer,
                  CommitStrategy commitStrategy, BooleanSupplier commitToKafkaSupplier) {
        this.kafkaFactory = kafkaFactory;
        this.lead = ignite.services().serviceProxy(LeadService.NAME, LeadService.class, false);
        this.config = config;
        this.serializer = serializer;
        this.commitStrategy = commitStrategy;
        nodeId = ignite.cluster().localNode().id();
        this.commitToKafkaSupplier = commitToKafkaSupplier;
    }

    @Override
    public void execute() {
        try (Consumer<ByteBuffer, ByteBuffer> consumer = createConsumer(config)) {
            registerRule(() -> pollAndCommitTransactionsBatch(consumer));
            registerRule(new PredicateRule(() -> commitOffsets(consumer), commitToKafkaSupplier));
            super.execute();
        }
    }

    private Consumer<ByteBuffer, ByteBuffer> createConsumer(SubscriberConfig config) {
        Consumer<ByteBuffer, ByteBuffer> consumer = kafkaFactory.consumer(config.getConsumerConfig());
        consumer.subscribe(Collections.singletonList(config.getRemoteTopic()));
        return consumer;
    }

    private void pollAndCommitTransactionsBatch(Consumer<ByteBuffer, ByteBuffer> consumer) {
        ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(POLL_TIMEOUT);

        List<TransactionScope> scopes = new ArrayList<>();

        for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
            TransactionScope transactionScope = serializer.deserialize(record.key());
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            buffer.put(transactionScope.getTransactionId(),
                    new TransactionData(transactionScope, record.value(), topicPartition, record.offset()));
            scopes.add(transactionScope);
            getCommittedOffset(topicPartition).notifyRead(record.offset());
        }
        if (!scopes.isEmpty()) {
            scopes.sort(Comparator.comparingLong(TransactionScope::getTransactionId));
        }
        approveAndCommitTransactionsBatch(scopes);
    }


    private void commitOffsets(Consumer consumer) {
        for (TopicPartition partition : committedOffsetMap.keySet()) {
            CommittedOffset offsetsForPartition = committedOffsetMap.get(partition);
            offsetsForPartition.compress();
            if (offsetsForPartition.getLastDenseCommit() >= 0) {
                OffsetAndMetadata offsetMetaInfo = new OffsetAndMetadata(offsetsForPartition.getLastDenseCommit());
                consumer.commitSync(Collections.singletonMap(partition, offsetMetaInfo));
            }
        }
    }

    private void approveAndCommitTransactionsBatch(List<TransactionScope> scopes) {
        List<Long> txIdsToCommit = lead.notifyRead(nodeId, scopes);

        if (!txIdsToCommit.isEmpty()) {
            txIdsToCommit.sort(Long::compareTo);
            commitStrategy.commit(txIdsToCommit, buffer);
            lead.notifyCommitted(txIdsToCommit);
            txIdsToCommit.stream().map(buffer::remove).forEach(e ->
                    getCommittedOffset(e.getTopicPartition()).notifyCommit(e.getOffset()));
        }
    }

    private CommittedOffset getCommittedOffset(TopicPartition topicPartition) {
        return committedOffsetMap.computeIfAbsent(topicPartition, k -> new CommittedOffset());
    }
}
