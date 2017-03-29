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
import com.epam.lathgertha.util.Serializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ReaderState {

    /**
     * TopicPartition - key all partition where read Reader, List<Offsets> - value list offsets read for partition
     */
    private Map<TopicPartition, List<Long>> partitionNumAndOffset = new HashMap<>();

    /**
     * txId - key, TransactionState - read data from kafka for txId
     */
    private Map<Long, TransactionState> entryState = new HashMap<>();

    private final Serializer serializer;

    public ReaderState(Serializer serializer) {
        this.serializer = serializer;
    }

    public void putToBuffer(ConsumerRecord<ByteBuffer, ByteBuffer> record) {
        TransactionState transactionState = new TransactionState(record);
        long transactionId = transactionState.getTransactionScope().getTransactionId();
        entryState.put(transactionId, transactionState);
    }

    public Map<Long, Map.Entry<TransactionScope, ByteBuffer>> getBuffer() {
        return entryState.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey,
                        (transactionState) -> transactionState.getValue().getScopeAndValue()));
    }

    public void removeFromBuffer(Long txId) {
        TransactionState entryState = this.entryState.remove(txId);
        TopicPartition topicPartition = entryState.getTopicPartition();
        addOffsetForTopicPartition(topicPartition, entryState.getOffset());
    }

    public void addOffsetForTopicPartition(TopicPartition topicPartition, Long offset) {
        List<Long> offsets = partitionNumAndOffset.computeIfAbsent(topicPartition, k -> new ArrayList<>());
        offsets.add(offset);
    }

    public Map<TopicPartition, List<Long>> getPartitionNumAndOffset() {
        return partitionNumAndOffset;
    }


    private class TransactionState {
        private ConsumerRecord<ByteBuffer, ByteBuffer> record;
        private TransactionScope transactionScope;

        TransactionState(ConsumerRecord<ByteBuffer, ByteBuffer> record) {
            this.record = record;
            transactionScope = serializer.deserialize(record.key());
        }

        TransactionScope getTransactionScope() {
            return transactionScope;
        }

        long getOffset() {
            return record.offset();
        }

        TopicPartition getTopicPartition() {
            return new TopicPartition(record.topic(), record.partition());
        }

        Map.Entry<TransactionScope, ByteBuffer> getScopeAndValue() {
            return new AbstractMap.SimpleImmutableEntry<>(transactionScope, record.value());
        }
    }
}
