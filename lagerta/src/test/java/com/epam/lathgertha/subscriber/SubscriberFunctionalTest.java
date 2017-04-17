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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class SubscriberFunctionalTest extends BaseFunctionalTest {
    private static final long TEST_TIMEOUT = 60_000;
    private static final int FIRST_KAFKA_PARTITION = 0;
    private static final int SECOND_KAFKA_PARTITION = 1;
    private static final int TRANSACTIONS_COUNT = 3;
    private static final int KEY_ONE = 1;
    private static final int KEY_TWO = 2;
    private static final int KEY_THREE = 3;
    private static final Integer VALUE_ONE = 11;
    private static final Integer VALUE_TWO = 12;
    private static final Integer VALUE_THREE = 13;

    private IgniteCache<Integer, Integer> txCache;
    private IgniteCache<Object, Object> txCountCache;

    @BeforeMethod
    public void initializeCaches() {
        txCache = ignite.cache(InCacheCommitter.TX_COMMIT_CACHE_NAME);
        txCountCache = ignite.cache(InCacheKafkaLogCommitter.COMMITTED_TRANSACTIONS_COUNT_CACHE_NAME);
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void committingAllSandTransactions() {
        writeValueToKafka(TOPIC, 0, KEY_ONE, VALUE_ONE, FIRST_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 1, KEY_TWO, VALUE_TWO, SECOND_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 2, KEY_THREE, VALUE_THREE, FIRST_KAFKA_PARTITION);

        awaitCacheUpdate(TRANSACTIONS_COUNT);

        assertEquals(txCache.get(KEY_ONE), VALUE_ONE);
        assertEquals(txCache.get(KEY_TWO), VALUE_TWO);
        assertEquals(txCache.get(KEY_THREE), VALUE_THREE);
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void committingTransactionsInProperOrder() {
        writeValueToKafka(TOPIC, 0, KEY_ONE, VALUE_ONE, FIRST_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 1, KEY_ONE, VALUE_TWO, SECOND_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 2, KEY_ONE, VALUE_THREE, FIRST_KAFKA_PARTITION);

        awaitCacheUpdate(TRANSACTIONS_COUNT);

        assertEquals(txCache.get(KEY_ONE), VALUE_THREE);
    }

    private void awaitCacheUpdate(int transactionsCount) {
        while (txCountCache.size() < transactionsCount);
    }
}
