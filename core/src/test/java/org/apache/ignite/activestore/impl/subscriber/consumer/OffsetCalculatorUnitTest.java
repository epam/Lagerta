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

package org.apache.ignite.activestore.impl.subscriber.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author Aleksandr_Meterko
 * @since 12/16/2016
 */
public class OffsetCalculatorUnitTest {

    private static final String TOPIC = "topic";

    /**
     * Transaction to commit go serially in 2 batches - 1,2,3 and 4,5. On first batch offset of all partitions should
     * be updated.
     */
    @Test
    public void serialTransactionsInPartitions() {
        OffsetCalculator calculator = new OffsetCalculator();

        Map<Long, TransactionWrapper> buffer = prepareTransactions();
        Map<TopicPartition, OffsetAndMetadata> firstPoll = calculator.calculateChangedOffsets(createTxIdsList(buffer, 1, 2, 3));
        Assert.assertEquals(3, firstPoll.size());
        Assert.assertEquals(1L, findOffset(firstPoll, 1));
        Assert.assertEquals(1L, findOffset(firstPoll, 2));
        Assert.assertEquals(1L, findOffset(firstPoll, 3));

        Map<TopicPartition, OffsetAndMetadata> secondPoll = calculator.calculateChangedOffsets(createTxIdsList(buffer, 4, 5));
        Assert.assertEquals(2, secondPoll.size());
        Assert.assertEquals(2L, findOffset(secondPoll, 1));
        Assert.assertEquals(2L, findOffset(secondPoll, 2));
    }

    /**
     * First batch contains 4,2,5 - only second partition should update offset. Second batch - 1,3.
     */
    @Test
    public void partitionsHaveBlockedTransactions() {
        OffsetCalculator calculator = new OffsetCalculator();

        Map<Long, TransactionWrapper> buffer = prepareTransactions();
        Map<TopicPartition, OffsetAndMetadata> firstPoll = calculator.calculateChangedOffsets(createTxIdsList(buffer, 2, 4, 5));
        Assert.assertEquals(1, firstPoll.size());
        Assert.assertEquals(2L, findOffset(firstPoll, 2));

        Map<TopicPartition, OffsetAndMetadata> secondPoll = calculator.calculateChangedOffsets(createTxIdsList(buffer, 1, 3));
        Assert.assertEquals(2, secondPoll.size());
        Assert.assertEquals(2L, findOffset(secondPoll, 1));
        Assert.assertEquals(1L, findOffset(secondPoll, 3));
    }

    /**
     * Row stands for partition and column its batch from poll. Number is transaction id.
     * p1 - 1 4
     * p2 - 2 5
     * p3 - 3
     */
    private Map<Long, TransactionWrapper> prepareTransactions() {
        Map<Long, TransactionWrapper> buffer = new HashMap<>();
        buffer.put(1L, mockTxWrapper(1, 1));
        buffer.put(4L, mockTxWrapper(1, 2));
        buffer.put(2L, mockTxWrapper(2, 1));
        buffer.put(5L, mockTxWrapper(2, 2));
        buffer.put(3L, mockTxWrapper(3, 1));
        return buffer;
    }

    private List<List<TransactionWrapper>> createTxIdsList(Map<Long, TransactionWrapper> buffer, long ... ids) {
        List<TransactionWrapper> result = new ArrayList<>(ids.length);
        for (long id : ids) {
            result.add(buffer.get(id));
        }
        return Collections.singletonList(result);
    }

    private long findOffset(Map<TopicPartition, OffsetAndMetadata> poll, int partition) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : poll.entrySet()) {
            if (entry.getKey().partition() == partition) {
                return entry.getValue().offset();
            }
        }
        Assert.fail("Did not find partition " + partition + " in result");
        return 0;
    }

    private TransactionWrapper mockTxWrapper(int partition, long offset) {
        TransactionWrapper result = mock(TransactionWrapper.class);
        doReturn(new TopicPartition(TOPIC, partition)).when(result).topicPartition();
        doReturn(offset).when(result).offset();
        return result;
    }

}
