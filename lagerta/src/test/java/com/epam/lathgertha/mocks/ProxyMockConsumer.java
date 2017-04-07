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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProxyMockConsumer<K, V> extends MockConsumer<K, V> {
    private final AtomicBoolean hasRecords = new AtomicBoolean(false);

    public ProxyMockConsumer(OffsetResetStrategy offsetResetStrategy) {
        super(offsetResetStrategy);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        super.subscribe(topics, listener);
        Map<TopicPartition, Long> offsets = topics
                .stream()
                .flatMap(
                        topic -> IntStream
                                .range(0, KafkaMockFactory.NUMBER_OF_PARTITIONS)
                                .mapToObj(i -> new TopicPartition(topic, i)))
                .collect(Collectors.toMap(Function.identity(), topicPartition -> 0L));
        rebalance(offsets.keySet());
        updateBeginningOffsets(offsets);
        updateEndOffsets(offsets);
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        if (!hasRecords.get()) {
            return ConsumerRecords.empty();
        }
        hasRecords.set(false);
        return super.poll(timeout);
    }

    @Override
    public void addRecord(ConsumerRecord<K, V> record) {
        super.addRecord(record);
        hasRecords.set(true);
    }
}
