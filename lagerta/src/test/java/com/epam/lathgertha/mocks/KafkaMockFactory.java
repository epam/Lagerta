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
package com.epam.lathgertha.mocks;

import com.epam.lathgertha.capturer.KeyTransformer;
import com.epam.lathgertha.capturer.ValueTransformer;
import com.epam.lathgertha.kafka.KafkaFactory;
import com.epam.lathgertha.kafka.SubscriberConfig;
import com.epam.lathgertha.util.Serializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteBufferSerializer;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public class KafkaMockFactory implements KafkaFactory {
    private static final Queue<ProxyMockConsumer<?, ?>> CONSUMERS = new ConcurrentLinkedQueue<>();

    public static final int NUMBER_OF_PARTITIONS = 2;

    private final KeyTransformer keyTransformer;
    private final ValueTransformer valueTransformer;
    private final SubscriberConfig config;
    private final Serializer serializer;

    private int specifiedNumberOfNodes = -1;

    public KafkaMockFactory(KeyTransformer keyTransformer, ValueTransformer valueTransformer, SubscriberConfig config,
                            Serializer serializer) {
        this.keyTransformer = keyTransformer;
        this.valueTransformer = valueTransformer;
        this.config = config;
        this.serializer = serializer;
    }

    private Consumer createConsumer() {
        ProxyMockConsumer consumer = new ProxyMockConsumer(OffsetResetStrategy.EARLIEST);

        for (Map.Entry<String, List<PartitionInfo>> entry : getInfos().entrySet()) {
            consumer.updatePartitions(entry.getKey(), entry.getValue());
        }
        CONSUMERS.add(consumer);
        return consumer;
    }

    private MockProducer createProducer() {
        Collection<PartitionInfo> partitionInfos = getInfos()
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        Cluster cluster = new Cluster(
                UUID.randomUUID().toString(),
                Collections.emptyList(),
                partitionInfos,
                Collections.emptySet(),
                Collections.emptySet()
        );
        return new MockProducer(cluster, true, null, new ByteBufferSerializer(), new ByteBufferSerializer());
    }

    private Map<String, List<PartitionInfo>> getInfos() {
        // Note: more topics are expected to be added.
        return Stream.of(config.getRemoteTopic())
                .flatMap(
                        topic -> IntStream
                                .range(0, NUMBER_OF_PARTITIONS)
                                .mapToObj(i -> new PartitionInfo(topic, i, null, null, null)))
                .collect(Collectors.groupingBy(PartitionInfo::topic));
    }

    @Override
    public <K, V> MockProducer<K, V> producer(Properties properties) {
        return createProducer();
    }

    @Override
    public <K, V> Consumer<K, V> consumer(Properties properties) {
        return createConsumer();
    }

    public InputProducer inputProducer(String topic, int partition) {
        if (specifiedNumberOfNodes > 0) {
            while (CONSUMERS.size() < specifiedNumberOfNodes) {
                //waiting for Readers subscribing
            }
        }
        ProxyMockConsumer consumer = existingOpenedConsumers(topic).get(partition);
        return new InputProducer(keyTransformer, valueTransformer, consumer, new TopicPartition(topic, partition),
            serializer);
    }

    private List<ProxyMockConsumer> existingOpenedConsumers(String topic) {
        List<ProxyMockConsumer> result = CONSUMERS
                .stream()
                .filter(consumer -> !consumer.closed())
                .filter(consumer -> consumer.subscription().contains(topic))
                .collect(Collectors.toList());
        if (result.isEmpty()) {
            throw new IllegalArgumentException("No consumer for topic " + topic);
        }
        return result;
    }

    /**
     * Don't support multiple subscribe on topic partition.
     */
    public Long getLastCommittedOffset(TopicPartition topicPartition) {
        List<ProxyMockConsumer> proxyMockConsumers = existingOpenedConsumers(topicPartition.topic());
        OffsetAndMetadata map = proxyMockConsumers.get(0).committed(topicPartition);
        return map == null ? null : map.offset();
    }

    public static void clearState() {
        CONSUMERS.clear();
    }

    public void setNumberOfNodes(int numberOfNodes) {
        this.specifiedNumberOfNodes = numberOfNodes;
    }
}
