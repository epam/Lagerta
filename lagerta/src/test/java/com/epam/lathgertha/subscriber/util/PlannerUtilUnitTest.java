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

import com.epam.lathgertha.subscriber.ConsumerTxScope;
import com.epam.lathgertha.subscriber.lead.CommittedTransactions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class PlannerUtilUnitTest {

    private static final String PLANNER_INFO = "plannerInfo";

    private static final UUID A = UUID.randomUUID();

    private static final UUID B = UUID.randomUUID();

    private static final String CACHE1 = "cache1";

    private static final String CACHE2 = "cache2";

    @Test(dataProvider = PLANNER_INFO)
    public void planningWorks(
            List<ConsumerTxScope> transactions,
            CommittedTransactions committed,
            Set<Long> inProgress,
            Map<UUID, List<Long>> expected) {
        transactions.sort(Comparator.comparingLong(ConsumerTxScope::getTransactionId));
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
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(A, 0, scope(CACHE1, 1)),
                consumerTx(A, 1, scope(CACHE1, 1))
        );
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0L, 1L);
        return new Object[]{transactions, new CommittedTransactions(), Sets.<Long>newHashSet(), expected};
    }

    // (a1 -> a2) + (a3 -> a4)
    private Object[] parallelSequencesSameCache() {
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(A, 0, scope(CACHE1, 1)),
                consumerTx(A, 1, scope(CACHE1, 1)),
                consumerTx(A, 2, scope(CACHE1, 2)),
                consumerTx(A, 3, scope(CACHE1, 2))
        );
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1, 2, 3);
        return new Object[]{transactions, new CommittedTransactions(), Sets.<Long>newHashSet(), expected};
    }

    // (a1 -> a2) + (a3 -> a4)
    private Object[] parallelSequencesDifferentCache() {
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(A, 0, scope(CACHE1, 1)),
                consumerTx(A, 1, scope(CACHE1, 1)),
                consumerTx(A, 2, scope(CACHE2, 2)),
                consumerTx(A, 3, scope(CACHE2, 2))
        );
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1, 2, 3);
        return new Object[]{transactions, new CommittedTransactions(), Sets.<Long>newHashSet(), expected};
    }

    // (a1 -> a2 -> a3) + (a2 -> a4)
    private Object[] sequenceWithFork() {
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(A, 0, scope(CACHE1, 1)),
                consumerTx(A, 1, scope(CACHE1, 1, 2)),
                consumerTx(A, 2, scope(CACHE1, 2)),
                consumerTx(A, 3, scope(CACHE1, 2))
        );
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1, 2, 3);
        return new Object[]{transactions, new CommittedTransactions(), Sets.<Long>newHashSet(), expected};
    }

    // (a1 -> a2 -> a3) + (a4 -> a5 -> a3)
    private Object[] sequenceWithJoin() {
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(A, 0, scope(CACHE1, 1)),
                consumerTx(A, 1, scope(CACHE1, 1)),
                consumerTx(A, 2, scope(CACHE2, 2)),
                consumerTx(A, 3, scope(CACHE1, 2), scope(CACHE2, 1))
        );
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1, 2, 3);
        return new Object[]{transactions, new CommittedTransactions(), Sets.<Long>newHashSet(), expected};
    }

    // (a1 -> a2 -> b1)
    private Object[] elementBlocked() {
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(A, 0, scope(CACHE1, 1)),
                consumerTx(A, 1, scope(CACHE1, 1)),
                consumerTx(B, 2, scope(CACHE1, 1))
        );
        Map<UUID, List<Long>> expected = nodeTransactions(A, 0, 1);
        return new Object[]{transactions, new CommittedTransactions(), Sets.<Long>newHashSet(), expected};
    }

    // (b1 -> a1 -> a2)
    private Object[] sequenceBlocked() {
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(B, 0, scope(CACHE1, 1)),
                consumerTx(A, 1, scope(CACHE1, 1)),
                consumerTx(A, 2, scope(CACHE1, 1))
        );
        Map<UUID, List<Long>> expected = nodeTransactions(B, 0);
        return new Object[]{transactions, new CommittedTransactions(), Sets.<Long>newHashSet(), expected};
    }

    // (a1 -> a2 -> a3) + (b1 -> a2)
    private Object[] sequenceWithBlockedElement() {
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(A, 0, scope(CACHE1, 1)),
                consumerTx(A, 1, scope(CACHE1, 1)),
                consumerTx(A, 3, scope(CACHE1, 1, 2)),
                consumerTx(B, 2, scope(CACHE1, 2))
        );
        Map<UUID, List<Long>> expected = NodeTransactionsBuilder.builder()
                .nodeTransactions(A, 0, 1)
                .nodeTransactions(B, 2)
                .build();
        return new Object[]{transactions, new CommittedTransactions(), Sets.<Long>newHashSet(), expected};
    }

    // (a1 -> a2 -> a3) + (a2 -> a4) + (b1 -> a4)
    private Object[] sequenceWithBlockedFork() {
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(A, 0, scope(CACHE1, 1)),
                consumerTx(A, 1, scope(CACHE1, 1, 2)),
                consumerTx(A, 2, scope(CACHE1, 1)),
                consumerTx(A, 4, scope(CACHE1, 2), scope(CACHE2, 1)),
                consumerTx(B, 3, scope(CACHE2, 1))
        );
        Map<UUID, List<Long>> expected = NodeTransactionsBuilder.builder()
                .nodeTransactions(A, 0, 1, 2)
                .nodeTransactions(B, 3)
                .build();
        return new Object[]{transactions, new CommittedTransactions(), Sets.<Long>newHashSet(), expected};
    }

    // (a1 -> a2 -> a3) + (a4 -> a5 -> a3) + (b1 -> a3)
    private Object[] sequenceWithBlockedJoin() {
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(A, 0, scope(CACHE1, 1)),
                consumerTx(A, 1, scope(CACHE1, 1, 2)),
                consumerTx(A, 2, scope(CACHE2, 1)),
                consumerTx(A, 3, scope(CACHE2, 1)),
                consumerTx(A, 5, scope(CACHE1, 1), scope(CACHE2, 1, 2)),
                consumerTx(B, 4, scope(CACHE2, 2))
        );
        Map<UUID, List<Long>> expected = NodeTransactionsBuilder.builder()
                .nodeTransactions(A, 0, 1, 2, 3)
                .nodeTransactions(B, 4)
                .build();
        return new Object[]{transactions, new CommittedTransactions(), Sets.<Long>newHashSet(), expected};
    }

    // (a1 -> a2) + (b1 -> a2)
    private Object[] sequenceBlockedFromOutsideCommitted() {
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(A, 0, scope(CACHE2, 1)),
                consumerTx(A, 2, scope(CACHE2, 1, 2)),
                consumerTx(B, 1, scope(CACHE2, 2))
        );
        CommittedTransactions committed = new CommittedTransactions();
        committed.addAll(Lists.newArrayList(0L));
        committed.compress();
        Map<UUID, List<Long>> expected = nodeTransactions(B, 1);
        return new Object[]{transactions, committed, Sets.<Long>newHashSet(), expected};
    }

    // (a1 -> a2) + (b1 -> a2)
    private Object[] sequenceBlockedFromOutsideCommittedAndInProgress() {
        List<ConsumerTxScope> transactions = Lists.newArrayList(
                consumerTx(A, 0, scope(CACHE2, 1)),
                consumerTx(A, 2, scope(CACHE2, 1, 2)),
                consumerTx(B, 1, scope(CACHE2, 2))
        );
        HashSet<Long> inProgress = Sets.newHashSet(0L, 1L);
        CommittedTransactions committed = new CommittedTransactions();
        committed.addAll(Lists.newArrayList(2L));
        committed.compress();
        return new Object[]{transactions, committed, inProgress, Collections.emptyMap()};
    }

    private Map<UUID, List<Long>> nodeTransactions(UUID nodeId, long... txIds) {
        return NodeTransactionsBuilder.builder().nodeTransactions(nodeId, txIds).build();
    }

    @SafeVarargs
    private static ConsumerTxScope consumerTx(UUID consumerId, long txId, Map.Entry<String, List>... cacheScopes) {
        return new ConsumerTxScope(consumerId, txId, Lists.newArrayList(cacheScopes));
    }

    private static Map.Entry<String, List> scope(String cacheName, Object... keys) {
        return new AbstractMap.SimpleImmutableEntry<>(cacheName, Lists.newArrayList(keys));
    }
}

class NodeTransactionsBuilder {

    private Map<UUID, List<Long>> map;

    private NodeTransactionsBuilder(Map<UUID, List<Long>> map) {
        this.map = map;
    }

    static NodeTransactionsBuilder builder() {
        return new NodeTransactionsBuilder(new HashMap<>());
    }

    Map<UUID, List<Long>> build() {
        return map;
    }

    NodeTransactionsBuilder nodeTransactions(UUID uuid, long... txIds) {
        List<Long> txs = Arrays.stream(txIds).boxed().collect(Collectors.toList());
        map.put(uuid, txs);
        return this;
    }
}
