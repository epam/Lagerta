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
package com.epam.lagerta.subscriber.util;

import com.epam.lagerta.capturer.TransactionScope;
import com.epam.lagerta.subscriber.ReaderTxScope;
import com.epam.lagerta.subscriber.lead.CommittedTransactions;
import com.epam.lagerta.subscriber.lead.Heartbeats;
import com.epam.lagerta.subscriber.lead.ReadTransactions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.epam.lagerta.util.DataProviderUtil.NodeTransactionsBuilder;
import static com.epam.lagerta.util.DataProviderUtil.cacheScope;
import static com.epam.lagerta.util.DataProviderUtil.list;
import static com.epam.lagerta.util.DataProviderUtil.txScope;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class PlannerUtilUnitTest {

    private static final String PLANNER_INFO = "plannerInfo";

    private static final UUID A = UUID.randomUUID();

    private static final UUID B = UUID.randomUUID();

    private static final String CACHE1 = "cache1";

    private static final String CACHE2 = "cache2";

    private static final CommittedTransactions EMPTY_COMMITTED = new CommittedTransactions();
    private static final HashSet<Long> EMPTY_IN_PROGRESS = Sets.newHashSet();
    private static final Set<UUID> EMPTY_LOST_READERS = Collections.emptySet();
    private static final Heartbeats HEARTBEATS = Mockito.mock(Heartbeats.class);

    @Test(dataProvider = PLANNER_INFO)
    public void planningWorks(
            ReadTransactions transactions,
            CommittedTransactions committed,
            Set<Long> inProgress,
            Map<UUID, List<Long>> expected) {
        transactions.makeReady();
        transactions.pruneCommitted(EMPTY_COMMITTED, HEARTBEATS, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS);
        List<ReaderTxScope> ready = PlannerUtil.plan(transactions, committed, inProgress, EMPTY_LOST_READERS);
        Map<UUID, List<Long>> plan = ready.stream()
                .collect(groupingBy(ReaderTxScope::getReaderId, toList()))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().stream().map(TransactionScope::getTransactionId).collect(toList())));
        assertEquals(plan, expected);
    }

    @DataProvider(name = PLANNER_INFO)
    public Object[][] plannerInfo() {
        return new Object[][]{
                simpleSequence(),
                parallelSequencesSameCache(),
                parallelSequencesDifferentCache(),
                sequenceWithFork(),
                sequenceWithJoin(),
                elementBlocked(),
                sequenceBlocked(),
                sequenceWithBlockedElement(),
                sequenceWithBlockedFork(),
                sequenceWithBlockedJoin(),
                sequenceBlockedFromOutsideCommitted(),
                sequenceBlockedFromOutsideCommittedAndInProgress()};
    }

    // (0 -> 1)
    private Object[] simpleSequence() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0L, cacheScope(CACHE1, 1L)),
                txScope(1L, cacheScope(CACHE1, 1L))));
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0L, 1L);
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (0 -> 1) + (2 -> 3)
    private Object[] parallelSequencesSameCache() {
        return sequence(list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1)),
                txScope(2, cacheScope(CACHE1, 2)),
                txScope(3, cacheScope(CACHE1, 2))));
    }

    // (0 -> 1) + (2 -> 3)
    private Object[] parallelSequencesDifferentCache() {
        return sequence(list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1)),
                txScope(2, cacheScope(CACHE2, 2)),
                txScope(3, cacheScope(CACHE2, 2))));
    }

    // (0 -> 1 -> 2) + (1 -> 3)
    private Object[] sequenceWithFork() {
        return sequence(list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1, 2)),
                txScope(2, cacheScope(CACHE1, 2)),
                txScope(3, cacheScope(CACHE1, 2))));
    }

    // (0 -> 1) + (2 -> 3)
    private Object[] sequenceWithJoin() {
        return sequence(list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1)),
                txScope(2, cacheScope(CACHE2, 2)),
                txScope(3, cacheScope(CACHE1, 2), cacheScope(CACHE2, 1))));
    }

    // (0 -> 1 -> 2)
    private Object[] elementBlocked() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1))));
        transactions.addAllOnNode(B, list(
                txScope(2, cacheScope(CACHE1, 1))));
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1);
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (0 -> 1 -> 2)
    private Object[] sequenceBlocked() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(B, list(
                txScope(0, cacheScope(CACHE1, 1))));
        transactions.addAllOnNode(A, list(
                txScope(1, cacheScope(CACHE1, 1)),
                txScope(2, cacheScope(CACHE1, 1))));
        Map<UUID, List<Long>> expected = nodeTransactions(B, 0);
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (0 -> 1 -> 3) + (2 -> 3)
    private Object[] sequenceWithBlockedElement() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1)),
                txScope(3, cacheScope(CACHE1, 1, 2))));
        transactions.addAllOnNode(B, list(
                txScope(2, cacheScope(CACHE1, 2))));
        Map<UUID, List<Long>> expected = NodeTransactionsBuilder.builder()
                .nodeTransactions(A, 0, 1)
                .nodeTransactions(B, 2)
                .build();
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (0 -> 1 -> 2) + (2 -> 4) + (3 -> 4)
    private Object[] sequenceWithBlockedFork() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1, 2)),
                txScope(2, cacheScope(CACHE1, 1)),
                txScope(4, cacheScope(CACHE1, 2),
                        cacheScope(CACHE2, 1))));
        transactions.addAllOnNode(B, list(
                txScope(3, cacheScope(CACHE2, 1))));
        Map<UUID, List<Long>> expected = NodeTransactionsBuilder.builder()
                .nodeTransactions(A, 0, 1, 2)
                .nodeTransactions(B, 3)
                .build();
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (0 -> 1 -> 5) + (2 -> 3 -> 5) + (4 -> 5)
    private Object[] sequenceWithBlockedJoin() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1, 2)),
                txScope(2, cacheScope(CACHE2, 1)),
                txScope(3, cacheScope(CACHE2, 1)),
                txScope(5, cacheScope(CACHE1, 1),
                        cacheScope(CACHE2, 1, 2))));
        transactions.addAllOnNode(B, list(
                txScope(4, cacheScope(CACHE2, 2))
        ));
        Map<UUID, List<Long>> expected = NodeTransactionsBuilder.builder()
                .nodeTransactions(A, 0, 1, 2, 3)
                .nodeTransactions(B, 4)
                .build();
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (0 -> 2) + (1 -> 2)
    private Object[] sequenceBlockedFromOutsideCommitted() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE2, 1)),
                txScope(2, cacheScope(CACHE2, 1, 2))));
        transactions.addAllOnNode(B, list(
                txScope(1, cacheScope(CACHE2, 2))));
        CommittedTransactions committed = new CommittedTransactions();
        committed.addAll(list(0L));
        committed.compress();
        transactions.makeReady();
        transactions.pruneCommitted(committed, HEARTBEATS, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS);
        Map<UUID, List<Long>> expected = nodeTransactions(B, 1);
        return new Object[]{transactions, committed, EMPTY_IN_PROGRESS, expected};
    }

    // (0 -> 2) + (1 -> 2)
    private Object[] sequenceBlockedFromOutsideCommittedAndInProgress() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE2, 1)),
                txScope(2, cacheScope(CACHE2, 1, 2))));
        transactions.addAllOnNode(B, list(
                txScope(1, cacheScope(CACHE2, 2))));
        HashSet<Long> inProgress = Sets.newHashSet(0L, 1L);
        CommittedTransactions committed = new CommittedTransactions();
        committed.addAll(Lists.newArrayList(2L));
        committed.compress();
        transactions.makeReady();
        transactions.pruneCommitted(committed, HEARTBEATS, EMPTY_LOST_READERS, EMPTY_IN_PROGRESS);
        return new Object[]{transactions, committed, inProgress, Collections.emptyMap()};
    }

    private static Object[] sequence(List<TransactionScope> list) {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list);
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1, 2, 3);
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    private static Map<UUID, List<Long>> nodeTransactions(UUID nodeId, long... txIds) {
        return NodeTransactionsBuilder.builder().nodeTransactions(nodeId, txIds).build();
    }
}
