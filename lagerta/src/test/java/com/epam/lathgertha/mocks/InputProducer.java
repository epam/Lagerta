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
import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.capturer.ValueTransformer;
import com.epam.lathgertha.util.Serializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import javax.cache.Cache;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputProducer {
    private static final Map<TopicPartition, Long> OFFSETS = new HashMap<>();

    private final KeyTransformer keyTransformer;
    private final ValueTransformer valueTransformer;
    private final ProxyMockConsumer consumer;
    private final TopicPartition topicPartition;
    private final Serializer serializer;

    public InputProducer(KeyTransformer keyTransformer, ValueTransformer valueTransformer, ProxyMockConsumer consumer,
                         TopicPartition topicPartition, Serializer serializer) {
        this.keyTransformer = keyTransformer;
        this.valueTransformer = valueTransformer;
        this.consumer = consumer;
        this.topicPartition = topicPartition;
        this.serializer = serializer;
    }

    public static void resetOffsets() {
        OFFSETS.clear();
    }

    @SuppressWarnings("unchecked")
    public void send(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates) {
        long offset = OFFSETS.computeIfAbsent(topicPartition, key -> 0L);
        TransactionScope key = keyTransformer.apply(transactionId, updates);
        List<List> value = valueTransformer.apply(updates);
        ConsumerRecord record = new ConsumerRecord(
            topicPartition.topic(),
            topicPartition.partition(),
            offset,
            serializer.serialize(key),
            serializer.serialize(value)
        );
        consumer.addRecord(record);
        OFFSETS.put(topicPartition, offset + 1);
    }
}
