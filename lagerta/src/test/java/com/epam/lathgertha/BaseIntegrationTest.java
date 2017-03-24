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

package com.epam.lathgertha;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.epam.lathgertha.resources.ReplicationClusters;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

public abstract class BaseIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseIntegrationTest.class);
    private static final long TX_WAIT_TIME = 1_000;
    private static final long LEAD_WAIT_TIME = 5_000;
    private static final String CACHE_NAME = "cache";
    private static final int START_INDEX = 0;
    private static final int END_INDEX = 100;

    private final ReplicationClusters replicationClusters = new ReplicationClusters();

    @BeforeSuite
    public void setUp() throws Exception {
        replicationClusters.setUp();
    }

    @AfterSuite
    public void tearDown() {
        replicationClusters.tearDown();
    }

    @AfterMethod
    public void cleanupResources() {
        replicationClusters.cleanUpClusters();
    }

    public Ignite mainGrid() {
        return replicationClusters.mainCluster().ignite();
    }

    public Ignite readerGrid() {
        return replicationClusters.readerCluster().ignite();
    }

    public <K, V> IgniteCache<K, V> getCache() {
        return mainGrid().cache(CACHE_NAME);
    }

    public Map<Integer, Integer> readValuesFromCache(Ignite ignite) {
        IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);
        Set<Integer> keys = IntStream
            .range(START_INDEX, END_INDEX)
            .boxed()
            .collect(Collectors.toSet());
        return cache.getAll(keys);
    }

    public void writeAndReplicate() {
        writeValuesToCache(mainGrid(), START_INDEX, END_INDEX);
    }

    public void writeAndReplicateSync() throws InterruptedException {
        writeValuesToCache(mainGrid(), START_INDEX, END_INDEX);
        awaitTransaction();
    }

    public void writeValuesToCache(Ignite ignite, int startIdx, int endIdx) {
        try (Transaction tx = ignite.transactions().txStart()) {
            IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

            for (int i = startIdx; i < endIdx; i++) {
                cache.put(i, i);
            }
            tx.commit();
        }
    }

    private void awaitTransaction() throws InterruptedException {
        Thread.sleep(TX_WAIT_TIME);
        LOGGER.debug("[T] SLEPT {}", TX_WAIT_TIME);
    }

    public void assertValuesInCache() {
        Map<Integer, Integer> updatedValues = readValuesFromCache(mainGrid());
        Map<Integer, Integer> expectedValues = IntStream
            .range(START_INDEX, END_INDEX)
            .boxed()
            .collect(Collectors.toMap(Function.identity(), Function.identity()));

        AssertJUnit.assertEquals(updatedValues, expectedValues);
        AssertJUnit.assertEquals(updatedValues, readValuesFromCache(readerGrid()));
    }

    public void awaitLead() throws InterruptedException {
        LOGGER.info("[T] start waiting lead");
        Thread.sleep(LEAD_WAIT_TIME);
        LOGGER.info("[T] finish waiting lead");
    }
}
