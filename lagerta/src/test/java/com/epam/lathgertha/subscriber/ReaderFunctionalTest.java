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
package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.BaseFunctionalTest;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class ReaderFunctionalTest extends BaseFunctionalTest {

    private static final long WAIT_KAFKA = 1_000;
    private final TopicPartition topicPartitionForWrite = new TopicPartition(TOPIC, 0);

    @Test
    public void commitToKafkaDenseOffsets() throws Exception {
        long expectedLastOffset = 3;
        long expectedMiddleOffset = 1;

        /**
         * txId  : 0   1   2   3
         * offset: 0   1   2   3
         */
        assertNull(kafkaMockFactory.getLastCommittedOffset(topicPartitionForWrite));
        Arrays.asList(0, 1).forEach(txId -> writeValueToKafka(TOPIC, txId, txId, txId));
        Thread.sleep(WAIT_KAFKA); //wait read records from kafka in Reader
        checkLastCommittedOffset(topicPartitionForWrite, expectedMiddleOffset);

        Arrays.asList(2, 3).forEach(txId -> writeValueToKafka(TOPIC, txId, txId, txId));
        Thread.sleep(WAIT_KAFKA); //wait read records from kafka in Reader
        checkLastCommittedOffset(topicPartitionForWrite, expectedLastOffset);
    }

    @Test
    public void commitToKafkaSparseOffsets() throws Exception {
        long expectedLastOffset = 7;

        /**
         * txId  : 4   5   6   7   0   2   1   3
         * offset: 0   1   2   3   4   5   6   7
         */
        Arrays.asList(4, 5, 6, 7).forEach(txId -> writeValueToKafka(TOPIC, txId, txId, txId));
        Thread.sleep(WAIT_KAFKA); //wait read records from kafka in Reader
        assertNull(kafkaMockFactory.getLastCommittedOffset(topicPartitionForWrite));

        Arrays.asList(0, 2, 1).forEach(txId -> writeValueToKafka(TOPIC, txId, txId, txId));
        Thread.sleep(WAIT_KAFKA); //wait read records from kafka in Reader
        assertNull(kafkaMockFactory.getLastCommittedOffset(topicPartitionForWrite));

        writeValueToKafka(TOPIC, 3, 3, 3);
        Thread.sleep(WAIT_KAFKA); //wait read records from kafka in Reader
        checkLastCommittedOffset(topicPartitionForWrite, expectedLastOffset);
    }

    private void checkLastCommittedOffset(TopicPartition expectedTopicPartition, Long expectedOffset) {
        Long offset = kafkaMockFactory.getLastCommittedOffset(expectedTopicPartition);
        assertNotNull(offset);
        assertEquals(offset, expectedOffset);
    }
}