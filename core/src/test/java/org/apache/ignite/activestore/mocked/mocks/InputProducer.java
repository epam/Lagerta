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

import org.apache.ignite.activestore.commons.Lazy;
import org.apache.ignite.activestore.impl.transactions.TransactionMessage;
import org.apache.ignite.activestore.commons.serializer.Serializer;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Aleksandr_Meterko
 * @since 12/28/2016
 */
public class InputProducer {

    private static final Lazy<TopicPartition, Long> OFFSETS = new Lazy<>(new C1<TopicPartition, Long>() {
        @Override public Long apply(TopicPartition partition) {
            return 0L;
        }
    });

    private final Serializer serializer;
    private final ProxyMockConsumer consumer;
    private final TopicPartition topicPartition;

    public InputProducer(Serializer serializer, ProxyMockConsumer consumer, TopicPartition topicPartition) {
        this.serializer = serializer;
        this.consumer = consumer;
        this.topicPartition = topicPartition;
    }

    public static void resetOffsets() {
        OFFSETS.clear();
    }

    @SuppressWarnings("unchecked")
    public void send(TransactionMessage message) {
        long offset = OFFSETS.get(topicPartition);
        ConsumerRecord record = new ConsumerRecord(topicPartition.topic(), topicPartition.partition(), offset,
            serializer.serialize(message.metadata), serializer.serialize(message.values));
        consumer.addRecord(record);
        OFFSETS.put(topicPartition, offset + 1);
    }
}
