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

package com.epam.lagerta.subscriber.lead;

import com.epam.lagerta.BaseMultiJVMIntegrationTest;
import com.epam.lagerta.base.jdbc.DataProviders;
import com.epam.lagerta.base.jdbc.common.PrimitivesHolder;
import com.epam.lagerta.services.LeadService;
import com.epam.lagerta.util.Atomic;
import com.epam.lagerta.util.AtomicsHelper;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.ignite.IgniteCache;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class LeadReinitionIntegrationTest extends BaseMultiJVMIntegrationTest {

    private static final long TEST_TIMEOUT = 60_000L;

    @Test(timeOut = TEST_TIMEOUT)
    public void leadChangedState() throws InterruptedException {
        awaitStartAllServerNodes();
        WriterToCache writeToCache = new WriterToCache();
        writeToCache.run();
        awaitTransactions();
        writeToCache.stop();
        long expectedLastDenseCommitted = writeToCache.getCount() - 1;

        //wait what lead state saved
        while (getSavedLeadState() != expectedLastDenseCommitted) {
            awaitTransactions();
        }
        waitAndCheckLeadLastDenseCommitted(expectedLastDenseCommitted);

        UUID oldIdNodeForLead = stopNodeWithService(LeadService.NAME);
        waitShutdownOneNode();
        awaitTransactions();

        if (oldIdNodeForLead == getNodeIdForService(LeadService.NAME)) {
            fail("Expected what lead is redeployed");
        }
        awaitTransactions();
        waitAndCheckLeadLastDenseCommitted(expectedLastDenseCommitted);
    }

    //todo issue #232
    // this test work if run it separate only
    //@Test(timeOut = TEST_TIMEOUT)
    public void leadChangedStateAfterDied() throws InterruptedException {
        awaitStartAllServerNodes();
        WriterToCache writeToCache = new WriterToCache();
        writeToCache.run();
        UUID oldIdNodeForLead = getNodeIdForService(LeadService.NAME);
        stopNodeWithService(LeadService.NAME);
        waitShutdownOneNode();
        awaitTransactions();
        if (oldIdNodeForLead == getNodeIdForService(LeadService.NAME)) {
            fail("Expected what lead is redeployed");
        }
        writeToCache.stop();
        awaitTransactions();
        long expectedLastDenseCommitted = writeToCache.getCount() - 1;
        waitAndCheckLeadLastDenseCommitted(expectedLastDenseCommitted);
    }

    private long getSavedLeadState() {
        Atomic<Long> atomic = AtomicsHelper.getAtomic(ignite(), LeadStateAssistantImpl.LEAD_STATE_CACHE);
        return atomic.get();
    }

    private void waitAndCheckLeadLastDenseCommitted(long expectedLastDenseCommitted) {
        LeadService leadService = ignite().services().serviceProxy(LeadService.NAME, LeadService.class, false);
        if (expectedLastDenseCommitted > CommittedTransactions.INITIAL_READY_COMMIT_ID) {
            while (leadService.getLastDenseCommitted() <= CommittedTransactions.INITIAL_READY_COMMIT_ID) {
                awaitTransactions();
            }
        }
        assertEquals(leadService.getLastDenseCommitted(), expectedLastDenseCommitted);
    }

    private static class WriterToCache {
        private volatile int count = 0;
        private Thread writeTask = new Thread(() -> {
            count = 0;
            IgniteCache<Integer, PrimitivesHolder> cache = ignite().cache(PrimitivesHolder.CACHE);
            while (!Thread.currentThread().isInterrupted()) {
                cache.put(count++, DataProviders.PH_1);
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
        });

        public void run() {
            writeTask.setDaemon(true);
            writeTask.start();
        }

        public void stop() {
            writeTask.interrupt();
        }

        public long getCount() {
            return count;
        }
    }
}