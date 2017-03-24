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

package org.apache.ignite.activestore.replication;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.activestore.BaseReplicationIntegrationTest;
import org.apache.ignite.activestore.commons.injection.InjectionForTests;
import org.apache.ignite.activestore.impl.publisher.LastTransactionListener;
import org.apache.ignite.activestore.impl.subscriber.ReceivedTransactionsListener;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadService;
import org.apache.ignite.transactions.Transaction;
import org.junit.After;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.lang.Thread.sleep;

/**
 * @author Andrei_Yakushin
 * @since 12/22/2016 4:39 PM
 */
public class BasicSynchronousReplicationIntegrationTest extends BaseReplicationIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicSynchronousReplicationIntegrationTest.class);

    protected static final String CACHE_NAME = "userCache";
    protected static final int INCREMENT_STEP = 100;
    public static final int SLEEP_TIME = 1000;

    // static because JUnit reinstantiates class for each test
    protected static int startIndex = 0;
    protected static int endIndex = INCREMENT_STEP;

    @After
    public void updateCounters() {
        startIndex += INCREMENT_STEP;
        endIndex += INCREMENT_STEP;
    }

    Map<Integer, Integer> readValuesFromCache(Ignite ignite) {
        IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);
        Set<Integer> keys = new HashSet<>();
        for (int i = startIndex; i < endIndex; i++) {
            keys.add(i);
        }
        return cache.getAll(keys);
    }

    void write(boolean withReplication) {
        writeValuesToCache(mainGrid(), withReplication, startIndex, endIndex, 0);
    }

    void writeAndReplicate(int modifier) {
        writeValuesToCache(mainGrid(), true, startIndex, endIndex, modifier);
    }

    void writeAndReplicateSync() {
        writeValuesToCache(mainGrid(), true, startIndex, endIndex, 0);
        waitTransaction();
    }

    private void waitTransaction() {
        LastTransactionListener listener = InjectionForTests.get(LastTransactionListener.class, mainGrid());
        Collection<Long> lastIds = listener.getTransactions();
        ReceivedTransactionsListener received = drGrid().services().serviceProxy(ReceivedTransactionsListener.SERVICE_NAME, ReceivedTransactionsListener.class, false);
        int i = 0;
        while (!received.receivedAll(lastIds)) {
            try {
                i++;
                sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                break;
            }
        }
        if (i > 0) {
            LOGGER.debug("[T] SLEPT {}", i * SLEEP_TIME);
        }
    }

    void writeValuesToCache(Ignite ignite, boolean withReplication, int startIdx, int endIdx, int modifier) {
        try (Transaction tx = ignite.transactions().txStart()) {
            IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);
            if (!withReplication) {
                cache = cache.withSkipStore();
            }
            for (int i = startIdx; i < endIdx; i++) {
                cache.put(i, i + modifier);
            }
            tx.commit();
        }
    }

    void assertCurrentStateOnServers() {
        assertCurrentStateOnServers(0);
    }

    void assertCurrentStateOnServers(int modifier) {
        Map<Integer, Integer> updatedValues = readValuesFromCache(mainGrid());
        for (int i = startIndex; i < endIndex; i++) {
            Assert.assertEquals(i + modifier, (int)updatedValues.get(i));
        }
        Assert.assertEquals(updatedValues, readValuesFromCache(drGrid()));
    }

    void waitLead() throws InterruptedException {
        LOGGER.info("[T] start waiting lead");
        LeadService lead = drGrid().services().serviceProxy(LeadService.SERVICE_NAME, LeadService.class, false);
        while (!lead.isInitialized()) {
            sleep(1_000);
        }
        LOGGER.info("[T] finish waiting lead");
    }
}
