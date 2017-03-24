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

package org.apache.ignite.activestore.impl.subscriber;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import javax.inject.Inject;
import org.apache.ignite.activestore.commons.injection.ActiveStoreService;
import org.apache.ignite.activestore.commons.serializer.Serializer;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactoryImpl;
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.services.ServiceContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author Andrei_Yakushin
 * @since 1/23/2017 12:58 PM
 */
public class ReceivedTransactionsListenerImpl extends ActiveStoreService implements ReceivedTransactionsListener {
    @Inject
    private transient KafkaFactoryImpl kafkaFactory;

    @Inject
    private transient DataRecoveryConfig dataRecoveryConfig;

    @Inject
    private transient Serializer serializer;

    private transient volatile boolean active;
    private transient Set<Long> receivedIds;

    @Override
    public boolean receivedAll(Collection<Long> ids) {
        return receivedIds.containsAll(ids);
    }

    @Override
    public void cancel(ServiceContext ctx) {
        active = false;
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        active = true;
        receivedIds = new GridConcurrentHashSet<>();

        Properties config = new Properties();
        config.putAll(dataRecoveryConfig.getConsumerConfig());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, ReceivedTransactionsListenerImpl.class.getSimpleName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        try (Consumer<ByteBuffer, ByteBuffer> consumer = kafkaFactory.consumer(config)) {
            consumer.subscribe(Arrays.asList(dataRecoveryConfig.getRemoteTopic(), dataRecoveryConfig.getReconciliationTopic()));
            while (active) {
                ConsumerRecords<ByteBuffer, ByteBuffer> poll = consumer.poll(500);
                for (ConsumerRecord<ByteBuffer, ByteBuffer> record : poll) {
                    TransactionMetadata metadata = serializer.deserialize(record.key());
                    receivedIds.add(metadata.getTransactionId());
                }
                consumer.commitSync();
            }
        }
    }
}
