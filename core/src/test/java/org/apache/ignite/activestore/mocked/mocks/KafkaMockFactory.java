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

package org.apache.ignite.activestore.mocked.mocks;

import org.apache.ignite.activestore.commons.serializer.Serializer;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.kafka.ConsumerProxyRetry;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.kafka.ProducerProxyRetry;
import org.apache.ignite.activestore.impl.subscriber.consumer.ConsumerAdapter;
import org.apache.ignite.activestore.impl.subscriber.consumer.ConsumerForTests;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteBufferSerializer;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Aleksandr_Meterko
 * @since 12/26/2016
 */
@SuppressWarnings("unchecked")
public class KafkaMockFactory implements KafkaFactory {
    // ToDo: Remove code duplication with KafkaFactoryForTests.

    public static final String REMOTE_ID = "remote";
    public static final String RECONCILIATION_ID = "recon";

    private static final Queue<ConsumerForTests<?, ?, ProxyMockConsumer<?, ?>>> CONSUMERS = new ConcurrentLinkedQueue<>();
    private static final Queue<MockProducer> PRODUCERS = new ConcurrentLinkedQueue<>();
    private static final int NUMBER_OF_PARTITIONS = 2;

    @Inject
    private Serializer serializer;

    @Inject
    private DataRecoveryConfig dataRecoveryConfig;

    private Consumer createConsumer() {
        ProxyMockConsumer consumer = new ProxyMockConsumer(OffsetResetStrategy.EARLIEST);
        for (Map.Entry<String, List<PartitionInfo>> entry : getInfos().entrySet()) {
            consumer.updatePartitions(entry.getKey(), entry.getValue());
        }
        ConsumerForTests testConsumer = new ConsumerForTests(consumer);

        CONSUMERS.add(testConsumer);
        return testConsumer;
    }

    private MockProducer createProducer() {
        Map<String, List<PartitionInfo>> infos = getInfos();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        for (List<PartitionInfo> infoEntry : infos.values()) {
            partitionInfos.addAll(infoEntry);
        }
        Cluster cluster = new Cluster(UUID.randomUUID().toString(), Collections.<Node>emptyList(), partitionInfos,
            Collections.<String>emptySet(), Collections.<String>emptySet());
        MockProducer producer = new MockProducer(cluster, true, null, new ByteBufferSerializer(), new ByteBufferSerializer());
        PRODUCERS.add(producer);
        return producer;
    }

    private Map<String, List<PartitionInfo>> getInfos() {
        Map<String, List<PartitionInfo>> partitionInfos = new HashMap<>();
        List<String> topics = Arrays.asList(dataRecoveryConfig.getLocalTopic(),
                dataRecoveryConfig.getReconciliationTopic(), dataRecoveryConfig.getRemoteTopic());
        for (String topic : topics) {
            List<PartitionInfo> partitions = new ArrayList<>();
            for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
                partitions.add(new PartitionInfo(topic, i, null, null, null));
            }
            partitionInfos.put(topic, partitions);
        }
        return partitionInfos;
    }

    @Override public <K, V> MockProducer<K, V> producer(Properties properties) {
        return createProducer();
    }

    @Override public <K, V> Producer<K, V> producer(Properties properties, Runnable onStop) {
        return new ProducerProxyRetry<>(this.<K, V>producer(properties), onStop);
    }

    @Override public <K, V> Consumer<K, V> consumer(Properties properties) {
        return createConsumer();
    }

    public InputProducer inputProducer(String topic, int partition) {
        ProxyMockConsumer consumer = existingOpenedConsumers(topic).get(partition);
        return new InputProducer(serializer, consumer, new TopicPartition(topic, partition));
    }

    private List<ProxyMockConsumer> existingOpenedConsumers(String topic) {
        List<ProxyMockConsumer> result = new ArrayList<>();
        for (ConsumerForTests<?, ?, ProxyMockConsumer<?, ?>> consumer : CONSUMERS) {
            ProxyMockConsumer proxyConsumer = consumer.getDelegate();

            if (proxyConsumer.subscription().contains(topic) && !proxyConsumer.closed()) {
                result.add(proxyConsumer);
            }
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("No consumer for topic " + topic);
        }
        return result;
    }

    @Override public <K, V> Consumer<K, V> consumer(Properties properties, Runnable onStop) {
        return new ConsumerProxyRetry<>(this.<K, V>consumer(properties), onStop);
    }

    @SuppressWarnings("unchecked")
    public static void substituteConsumers(ConsumerAdapter substitute) {
        for (ConsumerForTests consumer : CONSUMERS) {
            consumer.setSubstitute(substitute);
        }
    }

    public static void clearState() {
        CONSUMERS.clear();
        PRODUCERS.clear();
    }
}
