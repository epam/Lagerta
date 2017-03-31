package com.epam.lathgertha.subscriber;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.kafka.KafkaFactory;
import com.epam.lathgertha.kafka.KafkaLogCommitter;
import com.epam.lathgertha.kafka.SubscriberConfig;
import com.epam.lathgertha.mocks.KafkaMockFactory;
import com.epam.lathgertha.util.Serializer;
import com.epam.lathgertha.util.SerializerImpl;
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
import java.util.stream.Collectors;
import java.util.stream.LongStream;


public class SequentialCommitStrategyUnitTest {

    private static final String TOPIC = "testTopic";
    private static final String CACHE_NAME = "cache";
    private Serializer serializer = new SerializerImpl();
    private KafkaMockFactory kafkaMockFactory;
    private SubscriberConfig subscriberConfig;

    private SequentialCommitStrategy sequentialCommitStrategy;
    private MockProducer producer;
    private StatefulCommitter statefulCommitter;

    @BeforeClass
    public void init(){
        subscriberConfig = new SubscriberConfig();
        subscriberConfig.setRemoteTopic(TOPIC);
        subscriberConfig.setSubscriberId("1");
        subscriberConfig.setSuspendAllowed(false);
        kafkaMockFactory = new KafkaMockFactory(null, null, subscriberConfig, serializer);
    }

    @BeforeMethod
    public void initSequentialCommitStrategy() {
        producer = kafkaMockFactory.producer(null);
        KafkaFactory kafkaFactory = mock(KafkaFactory.class);
        when(kafkaFactory.producer(any())).thenReturn(producer);

        KafkaLogCommitter kafkaLogCommitter = new KafkaLogCommitter(kafkaFactory, subscriberConfig);
        statefulCommitter = new StatefulCommitter();
        Ignite ignite = mock(Ignite.class);
        doReturn(mock(IgniteServices.class)).when(ignite).services();
        sequentialCommitStrategy = new SequentialCommitStrategy(new CommitServitor(
                serializer,
                statefulCommitter,
                kafkaLogCommitter,
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