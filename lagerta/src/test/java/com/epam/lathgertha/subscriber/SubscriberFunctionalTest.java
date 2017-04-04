/*
 * Copyright (c) 2017. EPAM Systems.
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

package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.BaseFunctionalTest;
import org.apache.ignite.IgniteCache;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class SubscriberFunctionalTest extends BaseFunctionalTest {

    private static final int FIRST_KAFKA_PARTITION = 0;
    private static final int SECOND_KAFKA_PARTITION = 1;
    private static final int AWAIT_TIME = 100;
    private static final int TRANSACTIONS_COUNT = 3;
    private static final int KEY_ONE = 1;
    private static final int KEY_TWO = 2;
    private static final int KEY_THREE = 3;
    private static final Integer VALUE_ONE = 11;
    private static final Integer VALUE_TWO = 12;
    private static final Integer VALUE_THREE = 13;
    private IgniteCache<Integer, Integer> cache;
    private StatefulKafkaLogCommitter kafkaLogCommitter;

    @BeforeClass
    public void getStatefulKafkaLogCommitter() {
        kafkaLogCommitter = CLUSTER_MANAGER.getBean(StatefulKafkaLogCommitter.class);
    }

    @BeforeMethod
    public void initializeCache() {
        cache = ignite.cache(InCacheCommitter.TX_COMMIT_CACHE_NAME);
    }

    @AfterMethod
    public void cleanKafkaLogCommitter() {
        kafkaLogCommitter.cleanCommittedTransactionsCount();
    }

    @Test
    public void committingAllSandTransactions() throws InterruptedException {

        writeValueToKafka(TOPIC, 0, KEY_ONE, VALUE_ONE, FIRST_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 1, KEY_TWO, VALUE_TWO, SECOND_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 2, KEY_THREE, VALUE_THREE, FIRST_KAFKA_PARTITION);

        awaitCacheUpdate(TRANSACTIONS_COUNT);

        assertEquals(cache.get(KEY_ONE), VALUE_ONE);
        assertEquals(cache.get(KEY_TWO), VALUE_TWO);
        assertEquals(cache.get(KEY_THREE), VALUE_THREE);
    }

    @Test
    public void committingTransactionsInProperOrder() throws InterruptedException {
        writeValueToKafka(TOPIC, 0, KEY_ONE, VALUE_ONE, FIRST_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 1, KEY_ONE, VALUE_TWO, SECOND_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 2, KEY_ONE, VALUE_THREE, FIRST_KAFKA_PARTITION);

        awaitCacheUpdate(TRANSACTIONS_COUNT);

        assertEquals(cache.get(KEY_ONE), VALUE_THREE);
    }

    private void awaitCacheUpdate(int transactionsCount) {
        int committedTransactionsCount;
        do {
            committedTransactionsCount = kafkaLogCommitter.getCommittedTransactionsCount();
        } while (committedTransactionsCount < transactionsCount);
    }
}