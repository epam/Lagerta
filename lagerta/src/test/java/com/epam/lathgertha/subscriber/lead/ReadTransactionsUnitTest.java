/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.epam.lathgertha.subscriber.lead;

import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.subscriber.ConsumerTxScope;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.epam.lathgertha.subscriber.DataProviderUtil.cacheScope;
import static com.epam.lathgertha.subscriber.DataProviderUtil.list;
import static com.epam.lathgertha.subscriber.DataProviderUtil.txScope;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class ReadTransactionsUnitTest {
    private static final String DUPLICATES_PRUNING_PROVIDER = "duplicatesPruningProvider";
    private static final String LIST_OF_TRANSACTIONS = "listOfTransactions";
    private static final UUID NODE = java.util.UUID.randomUUID();
    private static final String CACHE = "cacheName";
    private static final Set<UUID> EMPTY_LOST_READERS = Collections.emptySet();
    private static final Set<Long> EMPTY_IN_PROGRESS = Collections.emptySet();

    private static final CommittedTransactions COMMITTED = Mockito.mock(CommittedTransactions.class);
    private static final List<Map.Entry<String, List>> CACHE_SCOPE = Mockito.mock(List.class);
    private static final Map.Entry<String, List> TX_SCOPE = Mockito.mock(Map.Entry.class);

    private static final long LAST_DENSE_COMMITTED = 1L;
    private static final long EXPECTED_LAST_DENSE_READ = 4L;

    // Reader letter name put as a second part of UUID.
    private static final UUID A = UUID.fromString("00000000-aaaa-0000-0000-000000000000");
    private static final UUID B = UUID.fromString("00000000-bbbb-0000-0000-000000000000");
    private static final UUID C = UUID.fromString("00000000-cccc-0000-0000-000000000000");

    private ReadTransactions read;
    private Heartbeats heartbeats;

    @BeforeMethod
    public void setUp() {
        read = new ReadTransactions();
        read.setReadyAndPrune(COMMITTED);
        heartbeats = Mockito.mock(Heartbeats.class);
        Mockito.when(COMMITTED.getLastDenseCommit()).thenReturn(LAST_DENSE_COMMITTED);
    }

    @DataProvider(name = LIST_OF_TRANSACTIONS)
    private Object[][] provideListsOfTransactions() {
        List<ConsumerTxScope> denseRead = list(
                consumerTxScope(2),
                consumerTxScope(3),
                consumerTxScope(4));
        return new Object[][]{
                {commonSizeLists(), denseRead},
                {diffSizeLists(), denseRead}};
    }

    private List<List<TransactionScope>> commonSizeLists() {
        return list(
                list(txScope(0, cacheScope(CACHE, 0L, 6L)),
                        txScope(1, cacheScope(CACHE, 5L, 7L))),
                list(txScope(2, cacheScope(CACHE, 1L, 2L)),
                        txScope(3, cacheScope(CACHE, 100L, 101L))),
                list(txScope(4, cacheScope(CACHE, 3L, 4L))));
    }

    private List<List<TransactionScope>> diffSizeLists() {
        return list(
                list(txScope(0, cacheScope(CACHE, 0L, 15L, 100L, 101L, 102L))),
                list(txScope(1, cacheScope(CACHE))),
                list(txScope(2, cacheScope(CACHE, 1L, 2L, 10L, 103L))),
                list(txScope(3, cacheScope(CACHE, 3L, 5L))),
                list(txScope(4, cacheScope(CACHE, 4L))),
                list(txScope(6, cacheScope(CACHE, 90L, 98L, 99L))),
                list(txScope(9, cacheScope(CACHE, 50L, 77L, 78L, 104L))),
                list(txScope(11, cacheScope(CACHE, 51L, 79L, 80L, 97L, 105L, 106L))),
                list(txScope(12, cacheScope(CACHE, 6L, 7L, 42L, 49L))));
    }

    @DataProvider(name = DUPLICATES_PRUNING_PROVIDER)
    private Object[][] provideForCheckDuplicatesPruning() {
        return new Object[][] {
            readerGotLostAndThenDied(),
            twoReadersGotLostOneGotBackToLife(),
            twoReadersGotLostOneGotBackToLifeFirstDied(),
            twoReadersLostThirdComesFirstBecomesDead(),
            threeLostThirdOneComesBackToLife()
        };
    }

    private Object[] readerGotLostAndThenDied() {
        // Given.
        TransactionScope tx0 = txScope(0, TX_SCOPE);
        TransactionScope tx1 = txScope(1, TX_SCOPE);
        Set<Long> inProgress = set(1L);
        Set<UUID> lostReaders = set(A);
        List<Runnable> happenedActions = list(
            () -> read.addAllOnNode(A, list(tx0)),
            () -> read.addAllOnNode(B, list(tx1)),
            () -> read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS),
            () -> read.addAllOnNode(B, list(tx0))
        );
        // Expected.
        ConsumerTxScope b0 = consumerTxScope(B, 0, false);
        ConsumerTxScope b1 = consumerTxScope(B, 1, false);
        Set<Long> expectedInProgress = Collections.emptySet();
        Set<UUID> expectedLostReaders = Collections.emptySet();
        List<ConsumerTxScope> expectedReadTxs = list(b0, b1);
        return new Object[] {
            happenedActions, lostReaders, inProgress, expectedInProgress,
            expectedLostReaders, expectedReadTxs
        };
    }

    private Object[] twoReadersGotLostOneGotBackToLife() {
        // Given.
        TransactionScope tx0 = txScope(0, TX_SCOPE);
        TransactionScope tx1 = txScope(1, TX_SCOPE);
        TransactionScope tx2 = txScope(2, TX_SCOPE);
        Set<Long> inProgress = set(1L);
        Set<UUID> lostReaders = set(A);
        List<Runnable> happenedActions = list(
            () -> read.addAllOnNode(A, list(tx0)),
            () -> read.addAllOnNode(B, list(tx0, tx1)),
            () -> read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS),
            () -> read.addAllOnNode(B, list(tx2))
        );
        // Expected.
        ConsumerTxScope b0 = consumerTxScope(B, 0, false);
        ConsumerTxScope b1 = consumerTxScope(B, 1, false);
        ConsumerTxScope b2 = consumerTxScope(B, 2, false);
        Set<Long> expectedInProgress = Collections.singleton(1L);
        Set<UUID> expectedLostReaders = Collections.singleton(A);
        List<ConsumerTxScope> expectedReadTxs = list(b0, b1, b2);
        return new Object[] {
            happenedActions, lostReaders, inProgress, expectedInProgress,
            expectedLostReaders, expectedReadTxs
        };
    }

    private Object[] twoReadersGotLostOneGotBackToLifeFirstDied() {
        // Given.
        TransactionScope tx0 = txScope(0, TX_SCOPE);
        TransactionScope tx1 = txScope(1, TX_SCOPE);
        TransactionScope tx2 = txScope(2, TX_SCOPE);
        Set<Long> inProgress = set(1L);
        Set<UUID> lostReaders = set(A);
        List<Runnable> happenedActions = list(
            () -> read.addAllOnNode(A, list(tx0)),
            () -> read.addAllOnNode(A, list(tx2)),
            () -> read.addAllOnNode(B, list(tx1)),
            () -> read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS),
            () -> read.addAllOnNode(B, list(tx0))
        );
        // Expected.
        ConsumerTxScope b0 = consumerTxScope(B, 0, false);
        ConsumerTxScope b1 = consumerTxScope(B, 1, false);
        ConsumerTxScope a2 = consumerTxScope(A, 2, true);
        Set<Long> expectedInProgress = Collections.emptySet();
        Set<UUID> expectedLostReaders = Collections.emptySet();
        List<ConsumerTxScope> expectedReadTxs = list(b0, b1, a2);
        return new Object[] {
            happenedActions, lostReaders, inProgress, expectedInProgress,
            expectedLostReaders, expectedReadTxs
        };
    }

    private Object[] twoReadersLostThirdComesFirstBecomesDead() {
        // Given.
        TransactionScope tx0 = txScope(0, TX_SCOPE);
        TransactionScope tx1 = txScope(1, TX_SCOPE);
        Set<Long> inProgress = set();
        Set<UUID> lostReaders = set(A, B);
        List<Runnable> happenedActions = list(
            () -> read.addAllOnNode(A, list(tx0, tx1)),
            () -> read.addAllOnNode(B, list(tx1)),
            () -> read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS),
            () -> read.addAllOnNode(C, list(tx0))
        );
        // Expected.
        ConsumerTxScope c0 = consumerTxScope(C, 0, false);
        ConsumerTxScope a1 = consumerTxScope(A, 1, true);
        Set<Long> expectedInProgress = Collections.emptySet();
        Set<UUID> expectedLostReaders = Collections.singleton(B);
        List<ConsumerTxScope> expectedReadTxs = list(c0, a1);
        return new Object[] {
            happenedActions, lostReaders, inProgress, expectedInProgress,
            expectedLostReaders, expectedReadTxs
        };
    }

    private Object[] threeLostThirdOneComesBackToLife() {
        // Given.
        TransactionScope tx0 = txScope(0, TX_SCOPE);
        TransactionScope tx1 = txScope(1, TX_SCOPE);
        TransactionScope tx2 = txScope(2, TX_SCOPE);
        Set<Long> inProgress = set(1L, 2L);
        Set<UUID> lostReaders = set(A, B);
        List<Runnable> happenedActions = list(
            () -> read.addAllOnNode(A, list(tx0, tx2)),
            () -> read.addAllOnNode(B, list(tx1, tx2)),
            () -> read.addAllOnNode(C, list(tx2)),
            () -> read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS),
            () -> read.addAllOnNode(C, list(tx1))
        );
        // Expected.
        ConsumerTxScope a0 = consumerTxScope(A, 0, false);
        ConsumerTxScope c1 = consumerTxScope(C, 1, false);
        ConsumerTxScope c2 = consumerTxScope(C, 2, false);
        Set<Long> expectedInProgress = Collections.emptySet();
        Set<UUID> expectedLostReaders = Collections.singleton(A);
        List<ConsumerTxScope> expectedReadTxs = list(a0, c1, c2);
        return new Object[] {
            happenedActions, lostReaders, inProgress, expectedInProgress,
            expectedLostReaders, expectedReadTxs
        };
    }

    @Test(dataProvider = DUPLICATES_PRUNING_PROVIDER)
    public void checkDuplicatesPruning(
        List<Runnable> happenedActions,
        Set<UUID> lostReaders,
        Set<Long> inProgress,
        Set<Long> expectedInProgress,
        Set<UUID> expectedLostReaders,
        List<ConsumerTxScope> expectedReadTxs
    ) {
        Mockito.doReturn(-1L).when(COMMITTED).getLastDenseCommit();
        happenedActions.forEach(Runnable::run);
        read.pruneCommitted(COMMITTED, heartbeats, lostReaders, inProgress);

        // ToDo: Enable when inProgress cleaning is fully implemented.
        //AssertJUnit.assertEquals(expectedInProgress, inProgress);
        AssertJUnit.assertEquals(expectedLostReaders, lostReaders);

        Iterator<ConsumerTxScope> it = read.iterator();

        for (ConsumerTxScope scope : expectedReadTxs) {
            ConsumerTxScope actualScope = it.next();

            assertEquals(actualScope.getConsumerId(), scope.getConsumerId());
            assertEquals(actualScope.getTransactionId(), scope.getTransactionId());
            assertEquals(actualScope.isOrphan(), scope.isOrphan());
        }
    }

    @Test(dataProvider = LIST_OF_TRANSACTIONS)
    public void pruningWorksWithAddManyLists(
            List<List<TransactionScope>> transactions,
            List<ConsumerTxScope> expectedDenseRead) {
        transactions.forEach(tx -> read.addAllOnNode(NODE, tx));
        read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS);
        long commitAfterCompress = read.getLastDenseRead();
        assertEquals(commitAfterCompress, EXPECTED_LAST_DENSE_READ);
    }

    @Test(dataProvider = LIST_OF_TRANSACTIONS)
    public void iteratorReturnsReadDenseTransactions(
            List<List<TransactionScope>> transactions,
            List<ConsumerTxScope> expectedDenseRead) {
        transactions.forEach(tx -> read.addAllOnNode(NODE, tx));
        read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS);
        List<Long> actual = Lists.newArrayList(read.iterator()).stream()
                .map(ConsumerTxScope::getTransactionId)
                .collect(Collectors.toList());
        List<Long> expected = expectedDenseRead.stream()
                .map(ConsumerTxScope::getTransactionId)
                .collect(Collectors.toList());
        assertEquals(actual, expected);
    }

    private static ConsumerTxScope consumerTxScope(UUID readerId, long transactionId, boolean isOrphan) {
        ConsumerTxScope result = new ConsumerTxScope(readerId, transactionId, CACHE_SCOPE);

        if (isOrphan) {
            result.markOrphan();
        }
        return result;
    }

    private static ConsumerTxScope consumerTxScope(long transactionId) {
        return consumerTxScope(NODE, transactionId, false);
    }

    @SafeVarargs
    private static <T> Set<T> set(T... objects) {
        return new HashSet<>(list(objects));
    }
}
