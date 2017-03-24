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
import com.epam.lathgertha.common.Scheduler;
import com.epam.lathgertha.kafka.KafkaFactory;
import com.epam.lathgertha.kafka.SubscriberConfig;
import com.epam.lathgertha.subscriber.lead.Lead;
import com.epam.lathgertha.util.Serializer;
import org.apache.ignite.Ignite;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Reader extends Scheduler {
    private static final int POLL_TIMEOUT = 200;

    private final KafkaFactory kafkaFactory;
    private final Lead lead;
    private final SubscriberConfig config;
    private final Serializer serializer;
    private final CommitStrategy commitStrategy;
    private final UUID nodeId;

    private final Map<Long, Map.Entry<TransactionScope, ByteBuffer>> buffer = new HashMap<>();

    public Reader(Ignite ignite, KafkaFactory kafkaFactory, Lead lead, SubscriberConfig config, Serializer serializer,
                  CommitStrategy commitStrategy) {
        this.kafkaFactory = kafkaFactory;
        this.lead = lead;
        this.config = config;
        this.serializer = serializer;
        this.commitStrategy = commitStrategy;
        nodeId = ignite.cluster().localNode().id();
    }

    @Override
    public void execute() {
        try (Consumer<ByteBuffer, ByteBuffer> consumer = createConsumer(config)) {
            registerRule(() -> pollAndCommitTransactionsBatch(consumer));
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
            buffer.put(transactionScope.getTransactionId(),
                    new SimpleImmutableEntry<>(transactionScope, record.value()));
            scopes.add(transactionScope);
        }

        approveAndCommitTransactionsBatch(nodeId, scopes);
    }

    private void approveAndCommitTransactionsBatch(UUID nodeId, List<TransactionScope> scopes) {
        final List<Long> txIdsToCommit = lead.notifyRead(nodeId, scopes);

        if (!txIdsToCommit.isEmpty()) {
            commitStrategy.commit(txIdsToCommit, buffer);
            lead.notifyCommitted(txIdsToCommit);
            txIdsToCommit.forEach(buffer::remove);
        }
    }
}
