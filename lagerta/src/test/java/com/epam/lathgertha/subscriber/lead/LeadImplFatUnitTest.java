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
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static com.epam.lathgertha.subscriber.DataProviderUtil.cacheScope;
import static com.epam.lathgertha.subscriber.DataProviderUtil.list;
import static com.epam.lathgertha.subscriber.DataProviderUtil.txScope;
import static org.mockito.Mockito.mock;

public class LeadImplFatUnitTest {

    private static final long TIMEOUT = 1000L;

    private static final LeadStateAssistant MOCK_STATE_ASSISTANT = mock(LeadStateAssistant.class);

    private static final UUID A = UUID.randomUUID();
    private static final UUID B = UUID.randomUUID();

    private static final String CACHE1 = "cache1";
    private static final String CACHE2 = "cache2";

    private LeadImpl lead;

    @BeforeMethod
    public void setUp() throws Exception {
        ReadTransactions read = new ReadTransactions();
        CommittedTransactions committed = new CommittedTransactions();
        lead = new LeadImpl(MOCK_STATE_ASSISTANT, read, committed);
        lead.updateState(committed);
        ForkJoinPool.commonPool().submit(() -> lead.execute());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        lead.stop();
    }

    @Test(timeOut = TIMEOUT)
    public void regressionTestOnBlockedTransactionsLogicInPlanner() {
        List<TransactionScope> aScope = list(
                txScope(0, cacheScope(CACHE2, 1L)),
                txScope(2, cacheScope(CACHE2, 1L, 2L)));

        List<TransactionScope> bScope = list(
                txScope(1, cacheScope(CACHE2, 2L)));

        notifyRead(B, bScope);
        notifyRead(A, aScope);
        assertPlanned(A, list(0L));
    }

    // (0 -> 2) + (1 -> 2)
    @Test(timeOut = TIMEOUT)
    public void sequenceBlockedFromOutside() {
        List<TransactionScope> aScope = list(
                txScope(0, cacheScope(CACHE2, 1L)),
                txScope(2, cacheScope(CACHE2, 1L, 2L)));

        List<TransactionScope> bScope = list(
                txScope(1, cacheScope(CACHE2, 2L)));

        notifyRead(A, aScope);
        notifyRead(B, bScope);
        assertPlanned(A, list(0L));
        notifyCommitted(A, list(0L));
        waitForLastDenseCommitted(0L);
        assertPlanned(B, list(1L));
        notifyCommitted(B, list(1L));
        waitForLastDenseCommitted(1L);
        assertPlanned(A, list(2L));
    }

    //    (0 -> 1 -> 2) + (1 -> 3) + (4 -> 5 -> 6) + (7 -> 8 -> 9)
    //      + (9 -> 10 -> 11) + (12 -> 14) + (13 -> 14)
    @Test(timeOut = TIMEOUT)
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

        List<Long> expectedFirstCommits = list(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 12L);

        notifyRead(A, aScope);
        notifyRead(B, bScope);
        assertPlanned(B, list(13L));
        assertEquals(notifyRead(B, list()), list());
        assertPlanned(A, expectedFirstCommits);
        assertEquals(notifyRead(A, list()), list());
        notifyCommitted(B, list(13L));
        assertEquals(notifyRead(B, list()), list());
        notifyCommitted(A, expectedFirstCommits);
        assertPlanned(A, list(14L));
        assertPlanned(B, list(10L));
        notifyCommitted(B, list(10L));
        assertPlanned(A, list(11L));
        notifyCommitted(A, list(11L));
        waitForLastDenseCommitted(13L);
    }

    private void assertPlanned(UUID uuid, List<Long> nonEmptyExpected) {
        List<Long> buffer = new ArrayList<>(nonEmptyExpected.size());
        do {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            buffer.addAll(notifyRead(uuid, list()));
        } while (buffer.isEmpty() || buffer.size() < nonEmptyExpected.size());
        assertEquals(buffer, nonEmptyExpected);
    }

    private List<Long> notifyRead(UUID uuid, List<TransactionScope> scope) {
        return lead.notifyRead(uuid, scope);
    }

    private void notifyCommitted(UUID readerId, List<Long> commits) {
        lead.notifyCommitted(readerId, commits);
    }

    private void waitForLastDenseCommitted(long expected) {
        long actual;
        do {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            actual = lead.getLastDenseCommitted();
        } while (actual != expected);
    }

    private static void assertEquals(List<Long> actual, List<Long> expected) {
        actual.sort(Long::compareTo);
        expected.sort(Long::compareTo);
        AssertJUnit.assertEquals(expected, actual);
    }
}
