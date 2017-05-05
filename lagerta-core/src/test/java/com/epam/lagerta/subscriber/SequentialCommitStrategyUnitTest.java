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

package com.epam.lagerta.subscriber;

import com.epam.lagerta.capturer.TransactionScope;
import com.epam.lagerta.kafka.KafkaFactory;
import com.epam.lagerta.kafka.KafkaLogCommitter;
import com.epam.lagerta.kafka.KafkaLogCommitterImpl;
import com.epam.lagerta.kafka.config.BasicTopicConfig;
import com.epam.lagerta.kafka.config.ClusterConfig;
import com.epam.lagerta.kafka.config.KafkaConfig;
import com.epam.lagerta.mocks.KafkaMockFactory;
import com.epam.lagerta.util.Serializer;
import com.epam.lagerta.util.SerializerImpl;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class SequentialCommitStrategyUnitTest {

    private static final String TOPIC = "testTopic";
    private static final String CACHE_NAME = "cache";
    private Serializer serializer = new SerializerImpl();
    private KafkaMockFactory kafkaMockFactory;
    private ClusterConfig clusterConfig;
    private BasicTopicConfig localIndexConfig;

    private SequentialCommitStrategy sequentialCommitStrategy;
    private MockProducer producer;
    private StatefulCommitter statefulCommitter;

    @BeforeClass
    public void init(){
        clusterConfig = new ClusterConfig();
        clusterConfig.setInputTopic(TOPIC);
        localIndexConfig = new BasicTopicConfig();
        localIndexConfig.setTopic(TOPIC);
        localIndexConfig.setKafkaConfig(mock(KafkaConfig.class));
        kafkaMockFactory = new KafkaMockFactory(null, null, clusterConfig, serializer);
    }

    @BeforeMethod
    public void initSequentialCommitStrategy() {
        producer = kafkaMockFactory.producer(null);
        KafkaFactory kafkaFactory = mock(KafkaFactory.class);
        when(kafkaFactory.producer(any())).thenReturn(producer);

        KafkaLogCommitter kafkaLogCommitter = new KafkaLogCommitterImpl(kafkaFactory, localIndexConfig);
        statefulCommitter = new StatefulCommitter();
        Ignite ignite = mock(Ignite.class);
        doReturn(mock(IgniteServices.class)).when(ignite).services();
        sequentialCommitStrategy = new SequentialCommitStrategy(new CommitServitor(
                serializer,
                statefulCommitter,
                kafkaLogCommitter,
                UUID.randomUUID(),
                ignite
        ));
    }

    @Test
    public void commitSingleTransaction() throws Exception {
        List<Long> expectedTxIds = Collections.singletonList(0L);
        List<Long> expectedKeys = Collections.singletonList(1L);
        List<String> expectedValues = Collections.singletonList("value1");

        Map<Long, TransactionData> transactionsBuffer = getTransactionsBuffer(CACHE_NAME,
                1, expectedKeys, expectedValues);
        sequentialCommitStrategy.commit(expectedTxIds, transactionsBuffer);

        checkExpectedResults(expectedTxIds, expectedKeys, expectedValues);
    }

    @Test
    public void commitTransactions() throws Exception {
        int countTx = 5;
        List<Long> expectedTxIds = LongStream.range(0, countTx).boxed().collect(Collectors.toList());
        List<Long> expectedKeys = LongStream.range(0, countTx).boxed().collect(Collectors.toList());
        List<String> expectedValues = expectedKeys.stream().map(k -> "value" + k).collect(Collectors.toList());

        Map<Long, TransactionData> transactionsBuffer = getTransactionsBuffer(CACHE_NAME,
                countTx, expectedKeys, expectedValues);
        sequentialCommitStrategy.commit(expectedTxIds, transactionsBuffer);
        checkExpectedResults(expectedTxIds, expectedKeys, expectedValues);
    }

    private void checkExpectedResults(List<Long> expectedTxIds, List<Long> expectedKeys, List<?> expectedValues) {
        List<ProducerRecord> wroteData = producer.history();
        List<Long> actualTxIds = wroteData.stream()
                .map(record -> record.timestamp())
                .collect(Collectors.toList());

        assertEquals(actualTxIds, expectedTxIds);
        assertEquals(statefulCommitter.getWrittenKeysAndValues().keySet(), new HashSet<>(expectedKeys));
        assertEquals(statefulCommitter.getWrittenKeysAndValues().values(), expectedValues);
    }

    private Map<Long, TransactionData> getTransactionsBuffer(String cacheName,
                                                             int countTx,
                                                             List<?> expectedKeys,
                                                             List<?> expectedValues
    ) {
        Map<Long, TransactionData> transactionsBuffer = new HashMap<>(countTx);
        for (int i = 0; i < countTx; i++) {
            Map.Entry<String, List> cacheScope =
                    new AbstractMap.SimpleEntry<String, List>(
                            cacheName,
                            Collections.singletonList(expectedKeys.get(i))
                    );
            TransactionScope txScope = new TransactionScope((long) i, Collections.singletonList(cacheScope));
            ByteBuffer serializedCacheValue = serializer.serialize(
                    Collections.singletonList(Collections.singletonList(expectedValues.get(i)))
            );
            TransactionData transactionScopeAndSerializedValues =
                    new TransactionData(
                            txScope,
                            serializedCacheValue,new TopicPartition(TOPIC,0),0L
                    );
            transactionsBuffer.put((long) i, transactionScopeAndSerializedValues);
        }
        return transactionsBuffer;
    }
}
