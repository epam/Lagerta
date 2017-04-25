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

package com.epam.lagerta.subscriber.lead;

import com.epam.lagerta.capturer.TransactionScope;
import com.epam.lagerta.subscriber.ReaderTxScope;
import com.google.common.collect.Lists;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.epam.lagerta.subscriber.DataProviderUtil.cacheScope;
import static com.epam.lagerta.subscriber.DataProviderUtil.list;
import static com.epam.lagerta.subscriber.DataProviderUtil.txScope;
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
        Mockito.when(COMMITTED.getLastDenseCommit()).thenReturn(LAST_DENSE_COMMITTED);
        read = new ReadTransactions();
        read.makeReady();
        read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS);
        heartbeats = Mockito.mock(Heartbeats.class);
    }

    @DataProvider(name = LIST_OF_TRANSACTIONS)
    private Object[][] provideListsOfTransactions() {
        List<ReaderTxScope> denseRead = list(
                readerTxScope(2),
                readerTxScope(3),
                readerTxScope(4));
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
        Set<Long> inProgress = set(0L);
        Set<UUID> lostReaders = set(A);
        List<Runnable> happenedActions = list(
            () -> read.addAllOnNode(A, list(tx0)),
            () -> read.addAllOnNode(B, list(tx1)),
            () -> read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS),
            () -> markInProgress(0L),
            () -> read.addAllOnNode(B, list(tx0))
        );
        // Expected: A reader died, tx 0 stopped being in progress. Orphan duplicate
        // of tx 0 is pruned.
        ReaderTxScope b0 = readerTxScope(B, 0, false);
        ReaderTxScope b1 = readerTxScope(B, 1, false);
        Set<Long> expectedInProgress = Collections.emptySet();
        Set<UUID> expectedLostReaders = Collections.emptySet();
        List<ReaderTxScope> expectedReadTxs = list(b0, b1);
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
        Set<Long> inProgress = set(0L);
        Set<UUID> lostReaders = set(A);
        List<Runnable> happenedActions = list(
            () -> read.addAllOnNode(A, list(tx0)),
            () -> read.addAllOnNode(B, list(tx0, tx1)),
            () -> read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS),
            () -> markInProgress(0L),
            () -> read.addAllOnNode(B, list(tx2))
        );
        // Expected: A reader remains lost, as there was no txs sent by B
        // which are also owned by sent after the A became lost.
        ReaderTxScope a0 = readerTxScope(B, 0, false);
        ReaderTxScope b1 = readerTxScope(B, 1, false);
        ReaderTxScope b2 = readerTxScope(B, 2, false);
        Set<Long> expectedInProgress = Collections.singleton(0L);
        Set<UUID> expectedLostReaders = Collections.singleton(A);
        List<ReaderTxScope> expectedReadTxs = list(a0, b1, b2);
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
        Set<Long> inProgress = set(0L);
        Set<UUID> lostReaders = set(A);
        List<Runnable> happenedActions = list(
            () -> read.addAllOnNode(A, list(tx0)),
            () -> read.addAllOnNode(A, list(tx2)),
            () -> read.addAllOnNode(B, list(tx1)),
            () -> read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS),
            () -> markInProgress(0L),
            () -> read.addAllOnNode(B, list(tx0))
        );
        // Expected. A claimed dead, tx 2 orphan, tx 0 stopped being in progress.
        ReaderTxScope b0 = readerTxScope(B, 0, false);
        ReaderTxScope b1 = readerTxScope(B, 1, false);
        ReaderTxScope a2 = readerTxScope(A, 2, true);
        Set<Long> expectedInProgress = Collections.emptySet();
        Set<UUID> expectedLostReaders = Collections.emptySet();
        List<ReaderTxScope> expectedReadTxs = list(b0, b1, a2);
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
        // Expected: A claimed dead, B remains lost.
        ReaderTxScope c0 = readerTxScope(C, 0, false);
        ReaderTxScope b1 = readerTxScope(B, 1, false);
        Set<Long> expectedInProgress = Collections.emptySet();
        Set<UUID> expectedLostReaders = Collections.singleton(B);
        List<ReaderTxScope> expectedReadTxs = list(c0, b1);
        return new Object[] {
            happenedActions, lostReaders, inProgress, expectedInProgress,
            expectedLostReaders, expectedReadTxs
        };
    }

    private Object[] threeLostThirdOneComesBackToLife() {
        // Given. Muliple nodes receive same txs due to rebalancing.
        TransactionScope tx0 = txScope(0, TX_SCOPE);
        TransactionScope tx1 = txScope(1, TX_SCOPE);
        TransactionScope tx2 = txScope(2, TX_SCOPE);
        Set<Long> inProgress = set(0L, 1L, 2L);
        Set<UUID> lostReaders = set(A, B);
        List<Runnable> happenedActions = list(
            () -> read.addAllOnNode(A, list(tx0, tx2)),
            () -> read.addAllOnNode(B, list(tx1, tx2)),
            () -> read.addAllOnNode(C, list(tx2)),
            () -> read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS),
            () -> markInProgress(0L, 1L, 2L),
            () -> read.addAllOnNode(C, list(tx1))
        );
        // Expected: B claimed dead, A remains lost, tx 2 duplicate from A pruned,
        // tx infos from B pruned.
        // Note: no knowledge which node progresses a txs at a moment - it can be any of them
        // as the ordering of merge procedure is not defined, though it places new duplicates after
        // the existing ones.
        ReaderTxScope a0 = readerTxScope(A, 0, false);
        ReaderTxScope c1 = readerTxScope(C, 1, false);
        ReaderTxScope c2 = readerTxScope(C, 2, false);
        Set<Long> expectedInProgress = set(0L, 2L);
        Set<UUID> expectedLostReaders = Collections.singleton(A);
        List<ReaderTxScope> expectedReadTxs = list(a0, c1, c2);
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
        List<ReaderTxScope> expectedReadTxs
    ) {
        Mockito.doReturn(CommittedTransactions.INITIAL_READY_COMMIT_ID).when(COMMITTED).getLastDenseCommit();
        happenedActions.forEach(Runnable::run);
        read.scheduleDuplicatesPruning();
        read.pruneCommitted(COMMITTED, heartbeats, lostReaders, inProgress);

        assertEquals(expectedInProgress, inProgress);
        assertEquals(expectedLostReaders, lostReaders);

        Iterator<ReaderTxScope> it = read.iterator();

        for (ReaderTxScope scope : expectedReadTxs) {
            ReaderTxScope actualScope = it.next();

            assertEquals(actualScope.getReaderId(), scope.getReaderId());
            assertEquals(actualScope.getTransactionId(), scope.getTransactionId());
            assertEquals(actualScope.isOrphan(), scope.isOrphan());
        }
    }

    @Test(dataProvider = LIST_OF_TRANSACTIONS)
    public void pruningWorksWithAddManyLists(
            List<List<TransactionScope>> transactions,
            List<ReaderTxScope> expectedDenseRead) {
        transactions.forEach(tx -> read.addAllOnNode(NODE, tx));
        read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS);
        long commitAfterCompress = read.getLastDenseRead();
        assertEquals(commitAfterCompress, EXPECTED_LAST_DENSE_READ);
    }

    @Test(dataProvider = LIST_OF_TRANSACTIONS)
    public void iteratorReturnsReadDenseTransactions(
            List<List<TransactionScope>> transactions,
            List<ReaderTxScope> expectedDenseRead) {
        transactions.forEach(tx -> read.addAllOnNode(NODE, tx));
        read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS);
        List<Long> actual = Lists.newArrayList(read.iterator()).stream()
                .map(ReaderTxScope::getTransactionId)
                .collect(Collectors.toList());
        List<Long> expected = expectedDenseRead.stream()
                .map(ReaderTxScope::getTransactionId)
                .collect(Collectors.toList());
        assertEquals(actual, expected);
    }

    @Test
    public void gapsInSparseReadAreCalculated() {
        List<TransactionScope> transactions = list(txScope(0), txScope(1), txScope(3), txScope(5), txScope(6));
        transactions.forEach(scope -> read.addAllOnNode(A, list(scope)));

        read.pruneCommitted(COMMITTED, heartbeats, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS);

        List<Long> actualGaps = read.gapsInSparseTransactions();
        List<Long> expectedGaps = list(2L, 4L);
        assertEquals(actualGaps, expectedGaps);
    }

    private void markInProgress(Long... txIds) {
        Set<Long> ids = set(txIds);

        for (ReaderTxScope scope : read) {
            if (ids.contains(scope.getTransactionId())) {
                scope.markInProgress();
            }
        }
    }

    private static ReaderTxScope readerTxScope(UUID readerId, long transactionId, boolean isOrphan) {
        ReaderTxScope result = new ReaderTxScope(readerId, transactionId, CACHE_SCOPE);

        if (isOrphan) {
            result.markOrphan();
        }
        return result;
    }

    private static ReaderTxScope readerTxScope(long transactionId) {
        return readerTxScope(NODE, transactionId, false);
    }

    @SafeVarargs
    private static <T> Set<T> set(T... objects) {
        return new HashSet<>(list(objects));
    }
}
