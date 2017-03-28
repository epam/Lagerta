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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import scala.tools.cmd.gen.AnyVals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataTransaction {

    // txId - key read from kafka, offset and partition num - value where read txId
    private Map<Long, ConsumerRecord> txIdsAndOffsetMetadata = new HashMap<>();

    // partitionNumber - key all partition where read Reader, List<Offsets> - value list offsets read for partition
    private Map<TopicPartition, List<Long>> partitionNumAndOffset = new HashMap<>();

    // txId - key read from kafka, Map.Entry<TransactionScope, ByteBuffer> - scope of transaction and value for csope
    private final Map<Long, Map.Entry<TransactionScope, ByteBuffer>> buffer = new HashMap<>();


    public void addOffsetsForPartition(TopicPartition topicPartition, Long ... offsets){
        List<Long> offsetForPartition = partitionNumAndOffset.get(topicPartition);
        if (offsetForPartition == null) {
            offsetForPartition = new ArrayList<>(Arrays.asList(offsets));
        } else {
            List<Long> c = Arrays.asList(offsets);
            offsetForPartition.addAll(c);
        }
        partitionNumAndOffset.put(topicPartition, offsetForPartition);
    }

    public void putToBuffer(Map.Entry<TransactionScope, ByteBuffer> txScope){
        long transactionId = txScope.getKey().getTransactionId();
        buffer.put(transactionId,txScope);
    }

    public Map.Entry<TransactionScope, ByteBuffer> removeFromBuffer(Long txId){
        return buffer.remove(txId);
    }

    public Map<Long, ConsumerRecord> getTxIdsAndOffsetMetadata() {
        return txIdsAndOffsetMetadata;
    }

    public Map<TopicPartition, List<Long>> getPartitionNumAndOffset() {
        return partitionNumAndOffset;
    }

    public Map<Long, Map.Entry<TransactionScope, ByteBuffer>> getBuffer() {
        return buffer;
    }
}
