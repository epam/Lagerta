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
import com.epam.lathgertha.util.SerializerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;


public class ReaderStateUnitTest {

    private final String DATA_PROVIDER_INFO_FOR_KAFKA = "infoForKafka";
    private Serializer serializer = new SerializerImpl();
    private String topic = "topic";

    private List<Map.Entry<String, List>> getCacheScope(long key) {
        Map.Entry<String, List> cacheScope =
                new AbstractMap.SimpleEntry<String, List>(
                        "someCache",
                        Collections.singletonList(key)
                );
        return Collections.singletonList(cacheScope);
    }

    private ConsumerRecord<ByteBuffer, ByteBuffer> getInfoForConsumerRecord(long txId, Object value, int partition, long offset) {
        ByteBuffer key  = serializer.serialize(new TransactionScope(txId, getCacheScope(txId)));
        ByteBuffer valueForKafka = serializer.serialize(value);
        return new ConsumerRecord<ByteBuffer, ByteBuffer> (topic, partition, offset, key, valueForKafka);
    }

    @DataProvider(name = DATA_PROVIDER_INFO_FOR_KAFKA)
    public Object[][] getInfoForConsumerRecord() {
        return new Object[][]{
                {0L,"value0",0,0},
                {1L,"value1",0,1},
                {2L,"value2",0,2},
        };
    }

    @Test(dataProvider = DATA_PROVIDER_INFO_FOR_KAFKA)
    public void putToBuffer(long txId, Object expectedValue, int partition, long offset) throws Exception {
        TransactionScope expectedTransactionScope = new TransactionScope(txId, getCacheScope(txId));
        ConsumerRecord<ByteBuffer, ByteBuffer> consumerRecord = getInfoForConsumerRecord(txId, expectedValue, partition, offset);

        ReaderState readerState = new ReaderState(serializer);
        readerState.putToBuffer(consumerRecord);
        Map<Long, Map.Entry<TransactionScope, ByteBuffer>> actualResult = readerState.getBuffer();

        assertEquals(actualResult.size(), 1);
        Map.Entry<TransactionScope, ByteBuffer> actualEntry = actualResult.get(expectedTransactionScope.getTransactionId());
        assertNotNull(actualEntry);
        assertEquals(actualEntry.getKey().getTransactionId(), expectedTransactionScope.getTransactionId());
        assertEquals(actualEntry.getKey().getScope(), expectedTransactionScope.getScope());
        assertEquals(serializer.deserialize(actualEntry.getValue()), expectedValue);
    }


    @Test(dataProvider = DATA_PROVIDER_INFO_FOR_KAFKA)
    public void removeFromBufferAddOffsetsForCommit(long txId, Object value, int partition, long expectedOffset) throws Exception {
        TopicPartition expectedPartition = new TopicPartition(topic, partition);
        ConsumerRecord<ByteBuffer, ByteBuffer> consumerRecord = getInfoForConsumerRecord(txId, value, partition, expectedOffset);

        ReaderState readerState = new ReaderState(serializer);
        readerState.putToBuffer(consumerRecord);

        Map<TopicPartition, List<Long>> actualPartitionNumAndOffset = readerState.getPartitionNumAndOffset();
        assertTrue(actualPartitionNumAndOffset.isEmpty());
        readerState.removeFromBuffer(txId);
        actualPartitionNumAndOffset = readerState.getPartitionNumAndOffset();
        assertFalse(actualPartitionNumAndOffset.isEmpty());
        long actualOffset = actualPartitionNumAndOffset.get(expectedPartition).get(0);
        assertEquals(actualOffset,expectedOffset);
    }
}