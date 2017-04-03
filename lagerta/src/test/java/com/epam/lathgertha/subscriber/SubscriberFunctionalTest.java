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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class SubscriberFunctionalTest extends BaseFunctionalTest {

    private final static int FIRST_KAFKA_PARTITION = 0;
    private final static int SECOND_KAFKA_PARTITION = 1;


    @Test(timeOut = 5000)
    public void committingAllSandTransactions() {

        int keyOne = 1;
        int valueOne = 11;
        int keyTwo = 2;
        int valueTwo = 12;
        int keyThree = 3;
        int valueThree = 13;

        writeValueToKafka(TOPIC, 0, keyOne, valueOne, FIRST_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 1, keyTwo, valueTwo, SECOND_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 2, keyThree, valueThree, FIRST_KAFKA_PARTITION);

        IgniteCache<Object, Object> cache = ignite.cache(InCacheCommitter.TX_COMMIT_CACHE_NAME);

        while (cache.get(keyOne) == null || cache.get(keyTwo) == null || cache.get(keyThree) == null);

        assertEquals(cache.get(keyOne), valueOne);
        assertEquals(cache.get(keyTwo), valueTwo);
        assertEquals(cache.get(keyThree), valueThree);
    }

    @Test(timeOut = 5000)
    public void committingTransactionsInProperOrder() throws InterruptedException {

        int key = 1;
        int valueOne = 11;
        int valueTwo = 12;
        int valueThree = 13;

        writeValueToKafka(TOPIC, 0, key, valueOne, FIRST_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 1, key, valueTwo, SECOND_KAFKA_PARTITION);
        writeValueToKafka(TOPIC, 2, key, valueThree, FIRST_KAFKA_PARTITION);

        IgniteCache<Object, Object> cache = ignite.cache(InCacheCommitter.TX_COMMIT_CACHE_NAME);

        int valueFromCache = 0;
        do {
            Thread.sleep(500);
            if (cache.get(key) != null) {
                valueFromCache = (int) cache.get(key);
            }

        } while (valueFromCache != valueThree);
    }
}