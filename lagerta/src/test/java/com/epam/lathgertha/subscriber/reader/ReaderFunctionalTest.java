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

import com.epam.lathgertha.BaseFunctionalTest;
import com.epam.lathgertha.mocks.ProxyMockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

public class ReaderFunctionalTest extends BaseFunctionalTest{

    @Test
    public void execute() throws Exception {
        int maxTxId = 7;
        List<Integer> listFirstTxIds = Arrays.asList(0, 4, 5, 6, maxTxId);
        for( int txId: listFirstTxIds) {
            writeValueToKafka(TOPIC, txId, txId, txId);
        }
        int offsetForMaxTxId = listFirstTxIds.indexOf(maxTxId);

        List<ProxyMockConsumer> proxyMockConsumers = kafkaMockFactory.existingOpenedConsumers(TOPIC);
        for (ProxyMockConsumer proxyMockConsumer : proxyMockConsumers) {
            OffsetAndMetadata lastCommitted = proxyMockConsumer.committed(new TopicPartition(TOPIC, 0));
            assertNull(lastCommitted);
        }

        for (int txId: Arrays.asList(2, 3, 1)) {
            writeValueToKafka(TOPIC, txId, txId, txId);
        }
        Thread.sleep(2_000); //wait read records from kafka in Reader
        checkLastCommittedOffset(offsetForMaxTxId);
    }

    private void checkLastCommittedOffset(long expectedOffset) {
        List<ProxyMockConsumer> proxyMockConsumers;
        proxyMockConsumers = kafkaMockFactory.existingOpenedConsumers(TOPIC);
        for (ProxyMockConsumer proxyMockConsumer : proxyMockConsumers) {
            OffsetAndMetadata lastCommitted = proxyMockConsumer.committed(new TopicPartition(TOPIC, 0));
            assertNotNull(lastCommitted);
            assertEquals(lastCommitted.offset(), expectedOffset);
        }
    }
}