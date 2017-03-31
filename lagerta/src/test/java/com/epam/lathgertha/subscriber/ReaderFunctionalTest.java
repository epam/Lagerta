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
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ReaderFunctionalTest extends BaseFunctionalTest {

    @Test
    public void execute() throws Exception {
        TopicPartition expectedTopicPartition = new TopicPartition(TOPIC, 0);
        int maxOffset = 7;
        List<Integer> listFirstTxIds = Arrays.asList(4, 5, 6, 7);
        for (int txId : listFirstTxIds) {
            writeValueToKafka(TOPIC, txId, txId, txId);
        }
        Thread.sleep(2_000); //wait read records from kafka in Reader
        assertNull(kafkaMockFactory.getLastCommittedOffset(expectedTopicPartition));
//        checkLastCommittedOffset(expectedTopicPartition,0);

        for (int txId : Arrays.asList(0, 2, 1)) {
            writeValueToKafka(TOPIC, txId, txId, txId);
        }
        Thread.sleep(2_000); //wait read records from kafka in Reader
        checkLastCommittedOffset(expectedTopicPartition, 0);

        writeValueToKafka(TOPIC, 3, 3, 3);
        Thread.sleep(2_000); //wait read records from kafka in Reader
        checkLastCommittedOffset(expectedTopicPartition, maxOffset);
    }

    private void checkLastCommittedOffset(TopicPartition expectedTopicPartition, long expectedOffset) {
        long offset = kafkaMockFactory.getLastCommittedOffset(expectedTopicPartition);
        assertEquals(offset, expectedOffset);
    }
}