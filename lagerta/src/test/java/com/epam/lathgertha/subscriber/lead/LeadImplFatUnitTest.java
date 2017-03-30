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

package com.epam.lathgertha.subscriber.lead;

import com.epam.lathgertha.capturer.TransactionScope;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BooleanSupplier;

import static com.epam.lathgertha.subscriber.DataProviderUtil.cacheScope;
import static com.epam.lathgertha.subscriber.DataProviderUtil.list;
import static com.epam.lathgertha.subscriber.DataProviderUtil.txScope;
import static org.testng.Assert.assertEquals;

public class LeadImplFatUnitTest {

    private static final UUID A = UUID.randomUUID();
    private static final UUID B = UUID.randomUUID();

    private static final String CACHE1 = "cache1";
    private static final String CACHE2 = "cache2";

    private LeadImpl lead;
    private ReadTransactions read;
    private DynamicRule dynamicRule;

    @BeforeMethod
    public void setUp() throws Exception {
        read = new ReadTransactions();
        dynamicRule = new DynamicRule();
        dynamicRule.setPredicate(() -> true);
        lead = new LeadImpl(read, new CommittedTransactions());
        lead.registerRule(dynamicRule);
        ForkJoinPool.commonPool().submit(() -> lead.execute());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        lead.stop();
    }

    // (a1 -> a2) + (b1 -> a2)
    @Test
    public void sequenceBlockedFromOutside() {
        List<TransactionScope> aScope = list(
                txScope(0, cacheScope(CACHE2, 1L)),
                txScope(2, cacheScope(CACHE2, 1L, 2L)));

        List<TransactionScope> bScope = list(
                txScope(1, cacheScope(CACHE2, 2L)));

        applyStatements(
                () -> notifyRead(A, aScope),
                () -> notifyRead(B, bScope),
                () -> assertEquals(notifyRead(A, list()), list(0L)),
                () -> notifyCommitted(list(0L)),
                () -> assertEquals(notifyRead(B, list()), list(1L)),
                () -> notifyCommitted(list(1L)),
                () -> assertEquals(notifyRead(A, list()), list(2L)));
    }

    //    (a1 -> a2 -> a3) + (a2 -> a4) + (a5 -> a6 -> a7) + (a8 -> a9 -> a7)
    //      + (a10 -> b1 -> a11) + (a12 -> a13) + (b2 -> a13)
    @Test
    public void forksJoinsAndBlockedPaths() {

        applyStatements(

        );
    }

    private List<Long> notifyRead(UUID uuid, List<TransactionScope> aScope) {
        dynamicRule.setPredicate(() -> read.getLastDenseRead() >= 0);
        return lead.notifyRead(uuid, aScope);
    }

    private void notifyCommitted(List<Long> committed) {
        lead.notifyCommitted(committed);
        dynamicRule.setPredicate(() -> lead.toCommit.size() > 0);
    }

    private void applyStatements(Runnable... statements) {
        for (Runnable run : statements) {
            dynamicRule.await();
            run.run();
        }
    }

    private static class DynamicRule implements Runnable {

        private CountDownLatch latch = new CountDownLatch(1);
        private BooleanSupplier predicate;

        @Override
        public void run() {
            if (predicate.getAsBoolean()) {
                latch.countDown();
            }
        }

        void await() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        void setPredicate(BooleanSupplier predicate) {
            this.predicate = predicate;
            this.latch = new CountDownLatch(1);
        }
    }
}
