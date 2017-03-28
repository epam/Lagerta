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
package com.epam.lathgertha.subscriber.reader;

import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.common.PredicateRule;
import com.epam.lathgertha.common.Scheduler;
import com.epam.lathgertha.kafka.KafkaFactory;
import com.epam.lathgertha.kafka.SubscriberConfig;
import com.epam.lathgertha.services.LeadService;
import com.epam.lathgertha.subscriber.CommitStrategy;
import com.epam.lathgertha.util.Serializer;
import org.apache.ignite.Ignite;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;

public class Reader extends Scheduler {
    private static final int POLL_TIMEOUT = 200;

    private int commitToKafkaEachIterate = 5;
    private long currentIterate = 0;
    private final BooleanSupplier CONDITION_COMMIT_TO_KAFKA = () -> ++currentIterate % commitToKafkaEachIterate == 0;

    private final KafkaFactory kafkaFactory;
    private final LeadService lead;
    private final SubscriberConfig config;
    private final Serializer serializer;
    private final CommitStrategy commitStrategy;
    private final UUID nodeId;

    private MetadataTransaction metadataTransaction;

    public Reader(Ignite ignite, KafkaFactory kafkaFactory, SubscriberConfig config, Serializer serializer,
                  CommitStrategy commitStrategy) {
        this.kafkaFactory = kafkaFactory;
        this.lead = ignite.services().serviceProxy(LeadService.NAME, LeadService.class, false);
        this.config = config;
        this.serializer = serializer;
        this.commitStrategy = commitStrategy;
        nodeId = ignite.cluster().localNode().id();
        metadataTransaction = new MetadataTransaction();
    }
    public Reader(Ignite ignite, KafkaFactory kafkaFactory, SubscriberConfig config, Serializer serializer,
                  CommitStrategy commitStrategy, int countIterateForCommit) {
        this(ignite,kafkaFactory,config,serializer,commitStrategy);
        commitToKafkaEachIterate = countIterateForCommit;
    }

    @Override
    public void execute() {
        try (Consumer<ByteBuffer, ByteBuffer> consumer = createConsumer(config)) {
            registerRule(()-> pollAndCommitTransactionsBatch(consumer) );
            registerRule(new PredicateRule(() -> commitOnKafka(consumer), CONDITION_COMMIT_TO_KAFKA));
            super.execute();
        }
    }

    private Consumer<ByteBuffer, ByteBuffer> createConsumer(SubscriberConfig config) {
        Properties consumerConfig = config.getConsumerConfig();
        String property = consumerConfig.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if (property == null || property.equalsIgnoreCase(String.valueOf(true))) {
            consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        }

        Consumer<ByteBuffer, ByteBuffer> consumer = kafkaFactory.consumer(consumerConfig);
        consumer.subscribe(Collections.singletonList(config.getRemoteTopic()));
        return consumer;
    }

    private void pollAndCommitTransactionsBatch(Consumer<ByteBuffer, ByteBuffer> consumer) {
        ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(POLL_TIMEOUT);

        List<TransactionScope> lastScopes = new ArrayList<>();

        for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
            TransactionScope transactionScope = serializer.deserialize(record.key());
            long transactionId = transactionScope.getTransactionId();
            metadataTransaction.putToBuffer(new SimpleImmutableEntry<>(transactionScope, record.value()));
            metadataTransaction.getTxIdsAndOffsetMetadata().put(transactionId, record);
            lastScopes.add(transactionScope);
        }

        approveAndCommitTransactionsBatch(lastScopes);
    }

    private void commitOnKafka(Consumer consumer) {
        Map<TopicPartition, List<Long>> partitionNumAndOffset = metadataTransaction.getPartitionNumAndOffset();
        for (TopicPartition partition : partitionNumAndOffset.keySet()) {
            List<Long> offsetsForPartition = partitionNumAndOffset.remove(partition);
            for (Long offset : offsetsForPartition) {
                OffsetAndMetadata offsetMetaInfo = new OffsetAndMetadata(offset);
                consumer.commitSync(Collections.singletonMap(partition, offsetMetaInfo));
            }
        }
    }

    private void approveAndCommitTransactionsBatch(List<TransactionScope> scopes) {
        final List<Long> txIdsToCommit = lead.notifyRead(nodeId, scopes);

        if (!txIdsToCommit.isEmpty()) {
            Map<Long, Map.Entry<TransactionScope, ByteBuffer>> buffer = metadataTransaction.getBuffer();
            commitStrategy.commit(txIdsToCommit, buffer);
            lead.notifyCommitted(txIdsToCommit);
            txIdsToCommit.forEach(buffer::remove);

            Map<Long, ConsumerRecord> txIdsAndOffsetMetadata = metadataTransaction.getTxIdsAndOffsetMetadata();
            for (Long txId:txIdsToCommit) {
                ConsumerRecord recordForTx = txIdsAndOffsetMetadata.remove(txId);
                metadataTransaction.addOffsetsForPartition(new TopicPartition(recordForTx.topic(),recordForTx.partition()), recordForTx.offset());
            }
        }
    }
}
