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

public class ReaderState {

    // partitionNumber - key all partition where read Reader, List<Offsets> - value list offsets read for partition
    private Map<TopicPartition, List<Long>> partitionNumAndOffset = new HashMap<>();
    private final Serializer serializer;

    ReaderState(Serializer serializer) {
        this.serializer = serializer;
    }

    private Map<Long, ReaderEntryState> entryState = new HashMap<>();


    public void putToBuffer(ConsumerRecord<ByteBuffer, ByteBuffer> record){
        TransactionScope transactionScope = serializer.deserialize(record.key());
        long transactionId = transactionScope.getTransactionId();
        entryState.put(transactionId, new ReaderEntryState(record));
    }

    public Map<Long, Map.Entry<TransactionScope, ByteBuffer>> getBuffer() {
        return entryState.entrySet().stream().collect(
                Collectors.toMap((entry) -> entry.getKey(),
                        (entry) -> entry.getValue().getScope()));
    }

    public Map.Entry<TransactionScope, ByteBuffer> removeFromBufferAndAddOffsetForCommit(Long txId) {
        ReaderEntryState entryState = this.entryState.remove(txId);
        TopicPartition topicPartition = entryState.getTopicPartition();
        List<Long> offsets = partitionNumAndOffset.get(topicPartition);
        if(offsets==null){
            offsets = new ArrayList<>();
            partitionNumAndOffset.put(topicPartition,offsets);
        }
        offsets.add(entryState.getOffset());
        return entryState.getScope();
    }

    public Map<TopicPartition, List<Long>> getPartitionNumAndOffset() {
        return partitionNumAndOffset;
    }


    public class ReaderEntryState{
        ConsumerRecord<ByteBuffer, ByteBuffer> record;

        // txId - key read from kafka, offset and partition num - value where read txId
//        private Map<Long, ConsumerRecord<ByteBuffer, ByteBuffer>> txIdsAndOffsetMetadata = new HashMap<>();
//
//        // txId - key read from kafka, Map.Entry<TransactionScope, ByteBuffer> - scope of transaction and value for csope
//        private final Map<Long, Map.Entry<TransactionScope, ByteBuffer>> buffer = new HashMap<>();


        public ReaderEntryState(ConsumerRecord<ByteBuffer, ByteBuffer> record) {
            this.record = record;
        }

        public long getOffset(){
            return record.offset();
        }

        public TopicPartition getTopicPartition(){
            return new TopicPartition(record.topic(),record.partition());
        }

        public Map.Entry<TransactionScope, ByteBuffer> getScope(){
            TransactionScope transactionScope = serializer.deserialize(record.key());
            return new AbstractMap.SimpleImmutableEntry<>(transactionScope, record.value());
        }


    }


//    public Map.Entry<TransactionScope, ByteBuffer> removeFromBufferAndAddOffsetForCommit(Long txId){
//        return buffer.remove(txId);
//    }
//
//    public Map<Long, ConsumerRecord> getTxIdsAndOffsetMetadata() {
//        return txIdsAndOffsetMetadata;
//    }
//
//    public Map<TopicPartition, List<Long>> getPartitionNumAndOffset() {
//        return partitionNumAndOffset;
//    }
//
//    public Map<Long, Map.Entry<TransactionScope, ByteBuffer>> getBuffer() {
//        return buffer;
//    }
}
