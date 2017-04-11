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
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static com.epam.lathgertha.subscriber.DataProviderUtil.cacheScope;
import static com.epam.lathgertha.subscriber.DataProviderUtil.list;
import static com.epam.lathgertha.subscriber.DataProviderUtil.txScope;
import static org.mockito.Mockito.mock;

public class LeadImplFatUnitTest {

    private static final LeadStateAssistantImpl MOCK_STATE_ASSISTANT = mock(LeadStateAssistantImpl.class);

    private static final UUID A = UUID.randomUUID();
    private static final UUID B = UUID.randomUUID();

    private static final String CACHE1 = "cache1";
    private static final String CACHE2 = "cache2";

    private LeadImpl lead;
    private ReadTransactions read;
    private CommittedTransactions commit;
    private Heartbeats heartbeats;
    private DynamicRule dynamicRule;

    @BeforeMethod
    public void setUp() throws Exception {
        read = new ReadTransactions();
        commit = new CommittedTransactions();
        heartbeats = new Heartbeats(LeadImpl.DEFAULT_HEARTBEAT_EXPIRATION_THRESHOLD);
        dynamicRule = new DynamicRule();
        dynamicRule.setPredicate(() -> true);
        lead = new LeadImpl(MOCK_STATE_ASSISTANT, read, commit, heartbeats);
        lead.updateState(commit);
        lead.registerRule(dynamicRule);
        ForkJoinPool.commonPool().submit(() -> lead.execute());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        lead.stop();
    }

    @Test
    public void regressionTestOnBlockedTransactionsLogicInPlanner() {
        List<TransactionScope> aScope = list(
                txScope(0, cacheScope(CACHE2, 1L)),
                txScope(2, cacheScope(CACHE2, 1L, 2L)));

        List<TransactionScope> bScope = list(
                txScope(1, cacheScope(CACHE2, 2L)));

        applyStatements(
                () -> notifyRead(B, bScope, -2),
                () -> notifyRead(A, aScope),
                () -> Uninterruptibles.sleepUninterruptibly(1_000, TimeUnit.MILLISECONDS),
                () -> assertEquals(notifyRead(A, list()), list(0L)));
    }

    private List<Long> notifyRead(UUID uuid, List<TransactionScope> scope, long previousLastDenseRead) {
        dynamicRule.setPredicate(() -> read.getLastDenseRead() > previousLastDenseRead);
        return lead.notifyRead(uuid, scope);
    }

    // (0 -> 2) + (1 -> 2)
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
                () -> notifyCommitted(A, list(0L)),
                () -> assertEquals(notifyRead(B, list()), list(1L)),
                () -> notifyCommitted(B, list(1L)),
                () -> assertEquals(notifyRead(A, list()), list(2L)));
    }

    //    (0 -> 1 -> 2) + (1 -> 3) + (4 -> 5 -> 6) + (7 -> 8 -> 9)
    //      + (9 -> 10 -> 11) + (12 -> 14) + (13 -> 14)
    @Test
    public void forksJoinsAndBlockedPaths() {
        List<TransactionScope> aScope = list(
                txScope(0, cacheScope(CACHE1, 1L)),
                txScope(1, cacheScope(CACHE1, 1L, 2L)),
                txScope(2, cacheScope(CACHE1, 1L)),
                txScope(3, cacheScope(CACHE1, 2L)),

                txScope(4, cacheScope(CACHE1, 3L)),
                txScope(5, cacheScope(CACHE1, 3L)),
                txScope(6, cacheScope(CACHE1, 3L, 4L)),
                txScope(7, cacheScope(CACHE1, 4L)),
                txScope(8, cacheScope(CACHE1, 4L)),

                txScope(9, cacheScope(CACHE1, 5L)),
                txScope(11, cacheScope(CACHE1, 5L)),

                txScope(12, cacheScope(CACHE2, 1L)),
                txScope(14, cacheScope(CACHE2, 1L, 2L)));

        List<TransactionScope> bScope = list(
                txScope(10, cacheScope(CACHE1, 5L)),
                txScope(13, cacheScope(CACHE2, 2L)));

        List<Long> expectedFirstCommits = list(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 12L, 14L);
        applyStatements(
                () -> notifyRead(A, aScope),
                () -> notifyRead(B, bScope),
                () -> notifyCommitted(A, list()),
                () -> assertEquals(notifyRead(B, list()), list(13L)),
                () -> assertEquals(notifyRead(B, list()), list()),
                () -> assertEquals(notifyRead(A, list()), expectedFirstCommits),
                () -> assertEquals(notifyRead(A, list()), list()),
                () -> notifyCommitted(B, list(13L)),
                () -> assertEquals(notifyRead(B, list()), list()),
                () -> notifyCommitted(A, expectedFirstCommits),
                () -> assertEquals(notifyRead(A, list()), list()),
                () -> assertEquals(notifyRead(B, list()), list(10L)),
                () -> notifyCommitted(B, list(10L)),
                () -> assertEquals(notifyRead(A, list()), list(11L)),
                () -> notifyCommitted(A, list(11L)),
                () -> Assert.assertTrue(commit.getLastDenseCommit() == 14L)
        );
    }

    private static void assertEquals(List<Long> actual, List<Long> expected) {
        actual.sort(Long::compareTo);
        expected.sort(Long::compareTo);
        Assert.assertEquals(actual, expected);
    }

    private List<Long> notifyRead(UUID uuid, List<TransactionScope> scope) {
        dynamicRule.setPredicate(() -> read.getLastDenseRead() > -1);
        return lead.notifyRead(uuid, scope);
    }

    private void notifyCommitted(UUID readerId, List<Long> committed) {
        lead.notifyCommitted(readerId, committed);
        dynamicRule.setPredicate(() -> committed.isEmpty() ||
                committed.stream().anyMatch(commit::contains));
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
