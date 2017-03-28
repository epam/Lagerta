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
package com.epam.lathgertha.subscriber.util;

import com.epam.lathgertha.subscriber.lead.CommittedTransactions;
import com.epam.lathgertha.subscriber.lead.ReadTransactions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.epam.lathgertha.subscriber.DataProviderUtil.*;
import static org.testng.Assert.assertEquals;

public class PlannerUtilUnitTest {

    private static final String PLANNER_INFO = "plannerInfo";

    private static final UUID A = UUID.randomUUID();

    private static final UUID B = UUID.randomUUID();

    private static final String CACHE1 = "cache1";

    private static final String CACHE2 = "cache2";

    private static final CommittedTransactions EMPTY_COMMITTED = new CommittedTransactions();
    private static final HashSet<Long> EMPTY_IN_PROGRESS = Sets.newHashSet();

    @Test(dataProvider = PLANNER_INFO)
    public void planningWorks(
            ReadTransactions transactions,
            CommittedTransactions committed,
            Set<Long> inProgress,
            Map<UUID, List<Long>> expected) {
        Map<UUID, List<Long>> plan = PlannerUtil.plan(transactions, committed, inProgress);
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

    // (a1 -> a2)
    private Object[] simpleSequence() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0L, cacheScope(CACHE1, 1L)),
                txScope(1L, cacheScope(CACHE1, 1L))));
        transactions.pruneCommitted(EMPTY_COMMITTED);
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0L, 1L);
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (a1 -> a2) + (a3 -> a4)
    private Object[] parallelSequencesSameCache() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1)),
                txScope(2, cacheScope(CACHE1, 2)),
                txScope(3, cacheScope(CACHE1, 2))));
        transactions.pruneCommitted(EMPTY_COMMITTED);
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1, 2, 3);
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (a1 -> a2) + (a3 -> a4)
    private Object[] parallelSequencesDifferentCache() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1)),
                txScope(2, cacheScope(CACHE2, 2)),
                txScope(3, cacheScope(CACHE2, 2))));
        transactions.pruneCommitted(EMPTY_COMMITTED);
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1, 2, 3);
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (a1 -> a2 -> a3) + (a2 -> a4)
    private Object[] sequenceWithFork() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1, 2)),
                txScope(2, cacheScope(CACHE1, 2)),
                txScope(3, cacheScope(CACHE1, 2))));
        transactions.pruneCommitted(EMPTY_COMMITTED);
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1, 2, 3);
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (a1 -> a2 -> a3) + (a4 -> a5 -> a3)
    private Object[] sequenceWithJoin() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1)),
                txScope(2, cacheScope(CACHE2, 2)),
                txScope(3, cacheScope(CACHE1, 2),
                        cacheScope(CACHE2, 1))));
        transactions.pruneCommitted(EMPTY_COMMITTED);
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1, 2, 3);
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (a1 -> a2 -> b1)
    private Object[] elementBlocked() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1))));
        transactions.addAllOnNode(B, list(
                txScope(2, cacheScope(CACHE1, 1))));
        transactions.pruneCommitted(EMPTY_COMMITTED);
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1);
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (b1 -> a1 -> a2)
    private Object[] sequenceBlocked() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(B, list(
                txScope(0, cacheScope(CACHE1, 1))));
        transactions.addAllOnNode(A, list(
                txScope(1, cacheScope(CACHE1, 1)),
                txScope(2, cacheScope(CACHE1, 1))));
        transactions.pruneCommitted(EMPTY_COMMITTED);
        Map<UUID, List<Long>> expected = nodeTransactions(B, 0);
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (a1 -> a2 -> a3) + (b1 -> a2)
    private Object[] sequenceWithBlockedElement() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE1, 1)),
                txScope(1, cacheScope(CACHE1, 1)),
                txScope(3, cacheScope(CACHE1, 1, 2))));
        transactions.addAllOnNode(B, list(
                txScope(2, cacheScope(CACHE1, 2))));
        transactions.pruneCommitted(EMPTY_COMMITTED);
        Map<UUID, List<Long>> expected = NodeTransactionsBuilder.builder()
                .nodeTransactions(A, 0, 1)
                .nodeTransactions(B, 2)
                .build();
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (a1 -> a2 -> a3) + (a2 -> a4) + (b1 -> a4)
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
        transactions.pruneCommitted(EMPTY_COMMITTED);
        Map<UUID, List<Long>> expected = NodeTransactionsBuilder.builder()
                .nodeTransactions(A, 0, 1, 2)
                .nodeTransactions(B, 3)
                .build();
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (a1 -> a2 -> a3) + (a4 -> a5 -> a3) + (b1 -> a3)
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
        transactions.pruneCommitted(EMPTY_COMMITTED);
        Map<UUID, List<Long>> expected = NodeTransactionsBuilder.builder()
                .nodeTransactions(A, 0, 1, 2, 3)
                .nodeTransactions(B, 4)
                .build();
        return new Object[]{transactions, EMPTY_COMMITTED, EMPTY_IN_PROGRESS, expected};
    }

    // (a1 -> a2) + (b1 -> a2)
    private Object[] sequenceBlockedFromOutsideCommitted() {
        ReadTransactions transactions = new ReadTransactions();
        transactions.addAllOnNode(A, list(
                txScope(0, cacheScope(CACHE2, 1)),
                txScope(2, cacheScope(CACHE2, 1, 2))));
        transactions.addAllOnNode(B, list(
                txScope(1, cacheScope(CACHE2, 2))));
        CommittedTransactions committed = new CommittedTransactions();
        committed.addAll(Lists.newArrayList(0L));
        committed.compress();
        transactions.pruneCommitted(committed);
        Map<UUID, List<Long>> expected = nodeTransactions(B, 1);
        return new Object[]{transactions, committed, EMPTY_IN_PROGRESS, expected};
    }

    // (a1 -> a2) + (b1 -> a2)
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
        transactions.pruneCommitted(committed);
        return new Object[]{transactions, committed, inProgress, Collections.emptyMap()};
    }

    private Map<UUID, List<Long>> nodeTransactions(UUID nodeId, long... txIds) {
        return NodeTransactionsBuilder.builder().nodeTransactions(nodeId, txIds).build();
    }
}
