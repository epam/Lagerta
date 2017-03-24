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

package org.apache.ignite.activestore.impl.subscriber.lead;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import org.apache.ignite.activestore.commons.Lazy;
import org.apache.ignite.activestore.commons.Reference;
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.*;

/**
 * @author Evgeniy_Ignatiev
 * @since 11/29/2016 7:03 PM
 */
@RunWith(Enclosed.class)
public class LeadPlannerUnitTest {
    private static final UUID a = UUID.randomUUID();

    private static final UUID b = UUID.randomUUID();

    private static final String cache1 = "cache1";

    private static final String cache2 = "cache2";

    public static class RegularWorkTests {
        private final LeadPlanner planner = new LeadPlanner(createState());

        @Before
        public void updateInitialContext() {
            planner.updateContext(-1, new TLongArrayList());
        }

        @Test
        public void noTxs() {
            assertResponsesReturned(planner);
        }

        /**
         * (a1 -> a2)
         */
        @Test
        public void simpleSequence() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 1, scope(cache1, 1));

            notifyTxsRead(planner, a1, a2);

            assertResponsesReturned(planner, response(a, 0, 1));
        }

        /**
         * (a1 -> a2) + (a3 -> a4)
         */
        @Test
        public void parallelSequencesSameCache() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 1, scope(cache1, 1));
            TxInfo a3 = txInfo(a, 2, scope(cache1, 2));
            TxInfo a4 = txInfo(a, 3, scope(cache1, 2));

            notifyTxsRead(planner, a1, a2, a3, a4);

            assertResponsesReturned(planner, response(a, 0, 1, 2, 3));
        }

        /**
         * (a1 -> a2) + (a3 -> a4)
         */
        @Test
        public void parallelSequencesDifferentCaches() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 1, scope(cache1, 1));
            TxInfo a3 = txInfo(a, 2, scope(cache2, 1));
            TxInfo a4 = txInfo(a, 3, scope(cache2, 1));

            notifyTxsRead(planner, a1, a2, a3, a4);

            assertResponsesReturned(planner, response(a, 0, 1, 2, 3));
        }

        /**
         * (a1 -> a2 -> a3) + (a2 -> a4)
         */
        @Test
        public void sequenceWithFork() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 1, scope(cache1, 1, 2));
            TxInfo a3 = txInfo(a, 2, scope(cache1, 1));
            TxInfo a4 = txInfo(a, 3, scope(cache1, 2));

            notifyTxsRead(planner, a1, a2, a3, a4);

            assertResponsesReturned(planner, response(a, 0, 1, 2, 3));
        }

        /**
         * (a1 -> a2 -> a3) + (a4 -> a5 -> a3)
         */
        @Test
        public void sequenceWithJoin() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 1, scope(cache1, 1));
            TxInfo a3 = txInfo(a, 2, scope(cache2, 1));
            TxInfo a4 = txInfo(a, 3, scope(cache2, 1));
            TxInfo a5 = txInfo(a, 4, scope(cache1, 1), scope(cache2, 1));

            notifyTxsRead(planner, a1, a2, a3, a4, a5);

            assertResponsesReturned(planner, response(a, 0, 1, 2, 3, 4));
        }

        /**
         * (a1 -> a2 -> b1)
         */
        @Test
        public void elementBlocked() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 1, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 2, scope(cache1, 1));

            notifyTxsRead(planner, a1, a2, b1);

            assertResponsesReturned(planner, response(a, 0, 1));
        }

        /**
         * (b1 -> a1 -> a2)
         */
        @Test
        public void sequenceBlocked() {
            TxInfo b1 = txInfo(b, 0, scope(cache1, 1));
            TxInfo a1 = txInfo(a, 1, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 2, scope(cache1, 1));

            notifyTxsRead(planner, a1, a2, b1);

            assertResponsesReturned(planner, response(b, 0));
        }

        /**
         * (a1 -> a2 -> a3) + (b1 -> a2)
         */
        @Test
        public void sequenceWithBlockedElement() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 1, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 2, scope(cache1, 2));
            TxInfo a3 = txInfo(a, 3, scope(cache1, 1, 2));

            notifyTxsRead(planner, a1, a2, a3, b1);

            assertResponsesReturned(planner, response(a, 0, 1), response(b, 2));
        }

        /**
         * (a1 -> a2 -> a3) + (a2 -> a4) + (b1 -> a4)
         */
        @Test
        public void sequenceWithBlockedFork() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 1, scope(cache1, 1, 2));
            TxInfo a3 = txInfo(a, 2, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 3, scope(cache2, 1));
            TxInfo a4 = txInfo(a, 4, scope(cache1, 2), scope(cache2, 1));

            notifyTxsRead(planner, a1, a2, a3, a4, b1);

            assertResponsesReturned(planner, response(a, 0, 1, 2), response(b, 3));
        }

        /**
         * (a1 -> a2 -> a3) + (a4 -> a5 -> a3) + (b1 -> a3)
         */
        @Test
        public void sequenceWithBlockedJoin() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 1, scope(cache1, 1));
            TxInfo a3 = txInfo(a, 2, scope(cache2, 1));
            TxInfo a4 = txInfo(a, 3, scope(cache2, 1));
            TxInfo b1 = txInfo(b, 4, scope(cache2, 2));
            TxInfo a5 = txInfo(a, 5, scope(cache1, 1), scope(cache2, 1, 2));

            notifyTxsRead(planner, a1, a2, a3, a4, a5, b1);

            assertResponsesReturned(planner, response(a, 0, 1, 2, 3), response(b, 4));
        }

        /**
         * (a1 -> a2) + (b1 -> a2)
         */
        @Test
        public void sequenceBlockedFromOutside() {
            TxInfo a1 = txInfo(a, 0, scope(cache2, 1));
            TxInfo b1 = txInfo(b, 1, scope(cache2, 2));
            TxInfo a2 = txInfo(a, 2, scope(cache2, 1, 2));

            notifyTxsRead(planner, a1, a2, b1);
            assertResponsesReturned(planner, response(a, 0), response(b, 1));

            markInProgress(planner, 0, 1);
            notifyTxCommitted(planner, b, 1);
            assertResponsesReturned(planner);

            notifyTxCommitted(planner, a, 0);
            assertResponsesReturned(planner, response(a, 2));
        }

        @Test
        public void sequenceWithLaterFilledGaps() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 2, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 1, scope(cache1, 2));

            notifyTxsRead(planner, a1, a2);
            assertResponsesReturned(planner, response(a, 0));

            markInProgress(planner, 0);
            notifyTxsRead(planner, b1);
            assertResponsesReturned(planner, response(b, 1));
        }

        @Test
        public void sequenceWithLaterFilledGapsCommittedEarly() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 2, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 1, scope(cache1, 2));

            notifyTxsRead(planner, a1, a2);
            assertResponsesReturned(planner, response(a, 0));

            markInProgress(planner, 0);
            notifyTxCommitted(planner, a, 0);
            notifyTxsRead(planner, b1);
            assertResponsesReturned(planner, response(a, 2), response(b, 1));
        }

        /**
         * (a1 -> a2 -> a3) + (a2 -> a4) + (a5 -> a6 -> a7) + (a8 -> a9 -> a7)
         *   + (a10 -> b1 -> a11) + (a12 -> a13) + (b2 -> a13)
         */
        @Test
        public void forksJoinsAndBlockedPaths() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 1, scope(cache1, 1, 2));
            TxInfo a3 = txInfo(a, 2, scope(cache1, 1));
            TxInfo a4 = txInfo(a, 3, scope(cache1, 2));

            TxInfo a5 = txInfo(a, 4, scope(cache1, 3));
            TxInfo a6 = txInfo(a, 5, scope(cache1, 3));
            TxInfo a7 = txInfo(a, 6, scope(cache1, 3, 4));
            TxInfo a8 = txInfo(a, 7, scope(cache1, 4));
            TxInfo a9 = txInfo(a, 8, scope(cache1, 4));

            TxInfo a10 = txInfo(a, 9, scope(cache1, 5));
            TxInfo b1 = txInfo(b, 10, scope(cache1, 5));
            TxInfo a11 = txInfo(a, 11, scope(cache1, 5));

            TxInfo a12 = txInfo(a, 12, scope(cache2, 1));
            TxInfo b2 = txInfo(b, 13, scope(cache2, 2));
            TxInfo a13 = txInfo(a, 14, scope(cache2, 1, 2));

            notifyTxsRead(planner, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, b1, b2);
            assertResponsesReturned(planner, response(a, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 12), response(b, 13));

            markInProgress(planner, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13);
            notifyTxCommitted(planner, b, 13);
            assertResponsesReturned(planner);

            markInProgress(planner, 13);
            notifyTxCommitted(planner, a, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 12);
            assertResponsesReturned(planner, response(a, 14), response(b, 10));

            markInProgress(planner, 14, 10);
            notifyTxCommitted(planner, b, 10);
            assertResponsesReturned(planner, response(a, 11));

            markInProgress(planner, 11);
            notifyTxCommitted(planner, a, 11);
            assertResponsesReturned(planner);
        }
    }

    @NotNull
    private static LeadPlanningState createState() {
        return new LeadPlanningState(new SimpleReference<Long>(), new SimpleReference<Boolean>());
    }

    public static class UpdateContextTests {
        private final LeadPlanner planner = new LeadPlanner(createState());

        @Test
        public void contextUpdatedAfterRestartSimpleSequence() {
            TxInfo a1 = txInfo(a, 5, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 7, scope(cache1, 2));
            TxInfo a3 = txInfo(a, 8, scope(cache1, 1));

            notifyTxsRead(planner, a1, a2, a3);
            markInProgress(planner, 7);
            notifyTxCommitted(planner, a, 7);
            planner.updateContext(4, ids(6));
            assertResponsesReturned(planner, response(a, 5, 8));
        }

        @Test
        public void contextUpdatedAfterRestartWithGapBeforeNewlyRegistered() {
            TxInfo a1 = txInfo(a, 4, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 6, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 8, scope(cache1, 2));
            TxInfo b2 = txInfo(b, 7, scope(cache1, 2));

            notifyTxsRead(planner, a1, a2, b1);
            planner.updateContext(3, ids(5));
            assertResponsesReturned(planner, response(a, 4, 6));

            markInProgress(planner, 4, 6);
            notifyTxsRead(planner, b2);
            assertResponsesReturned(planner, response(b, 7, 8));
        }

        @Test
        public void contextUpdatedAfterRestartWithIdsCommittedBeforeRestarting() {
            TxInfo a1 = txInfo(a, 4, scope(cache1, 1, 2));
            TxInfo a2 = txInfo(a, 6, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 5, scope(cache1, 2));

            planner.updateContext(4, ids());
            notifyTxsRead(planner, a1, a2, b1);

            assertResponsesReturned(planner, response(a, ids(6), ids(4)), response(b, 5));
        }

        @Test
        public void contextUpdatedAfterRestartWithIdsCommittedWhileRestarting() {
            TxInfo a1 = txInfo(a, 4, scope(cache1, 1, 2));
            TxInfo a2 = txInfo(a, 6, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 5, scope(cache1, 2));

            planner.updateContext(4, ids());
            notifyTxsRead(planner, a1, a2, b1);
            notifyTxCommitted(planner, a, 4);

            assertResponsesReturned(planner, response(a, ids(6), ids(4)), response(b, 5));
        }
    }

    public static class TransactionOwnershipTransferTests {
        private final LeadPlanner planner = new LeadPlanner(createState());

        @Before
        public void updateInitialContext() {
            planner.updateContext(-1, new TLongArrayList());
        }

        @Test
        public void txsOwnershipTransferredCommittedByOldConsumer() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 0, scope(cache1, 1));

            notifyTxsRead(planner, a1);
            markInProgress(planner, 0);
            notifyTxsRead(planner, b1);
            notifyTxCommitted(planner, a, 0);

            assertResponsesReturned(planner, response(b, null, ids(0)));
        }

        @Test
        public void txsOwnershipTransferredCommittedByNewConsumer() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 0, scope(cache1, 1));
            TxInfo a2 = txInfo(a, 1, scope(cache1, 1));

            notifyTxsRead(planner, a1);
            markInProgress(planner, 0);
            notifyTxsRead(planner, b1);
            notifyTxCommitted(planner, b, 0);
            notifyTxsRead(planner, a2);

            assertResponsesReturned(planner, response(a, 1));
        }

        @Test
        public void txsOwnershipTransferredFromCrashedConsumer() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 0, scope(cache1, 1));

            notifyTxsRead(planner, a1);
            markInProgress(planner, 0);
            // Consumer a crashed and stopped communicating with lead.
            planner.registerOutOfOrderConsumer(a);
            notifyTxsRead(planner, b1);

            assertResponsesReturned(planner, response(b, 0));
        }

        @Test
        public void txsOwnershipTransferUnblocksFollowingTxs() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 1, scope(cache1, 1));
            TxInfo b2 = txInfo(b, 0, scope(cache1, 1));

            notifyTxsRead(planner, a1, b1);
            markInProgress(planner, 0);
            planner.registerOutOfOrderConsumer(a);
            notifyTxsRead(planner, b2);

            assertResponsesReturned(planner, response(b, 0, 1));
        }

        @Test
        public void crashedConsumerCleanedAfterTxsOwnershipTransfer() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 0, scope(cache1, 1));

            notifyTxsRead(planner, a1);
            markInProgress(planner, 0);
            planner.registerOutOfOrderConsumer(a);
            notifyTxsRead(planner, b1);

            Set<UUID> outOfOrderConsumers = Collections.singleton(a);
            Set<UUID> result = planner.getFinallyDeadConsumers(outOfOrderConsumers);

            Assert.assertEquals(result, outOfOrderConsumers);
        }

        @Test
        public void committedByOldConsumerBeforeTransferAcknowledgedByLead() {
            TxInfo a1 = txInfo(a, 0, scope(cache1, 1));
            TxInfo b1 = txInfo(b, 0, scope(cache1, 1));

            notifyTxsRead(planner, a1);
            markInProgress(planner, 0);
            notifyTxCommitted(planner, a, 0);
            notifyTxsRead(planner, b1);

            assertResponsesReturned(planner, response(b, null, ids(0)));
        }
    }

    private static void markInProgress(LeadPlanner planner, long... txIds) {
        planner.markInProgressTransactions(ids(txIds));
    }

    private static void notifyTxsRead(LeadPlanner planner, TxInfo... txs) {
        Lazy<UUID, List<TxInfo>> txPerNode = new Lazy<>(new IgniteClosure<UUID, List<TxInfo>>() {
            @Override public List<TxInfo> apply(UUID uuid) {
                return new ArrayList<>();
            }
        });

        for (TxInfo txInfo : txs) {
            txPerNode.get(txInfo.consumerId()).add(txInfo);
        }
        for (Map.Entry<UUID, List<TxInfo>> notifyBach : txPerNode) {
            planner.registerNew(notifyBach.getValue());
        }
    }

    @SafeVarargs
    private static void assertResponsesReturned(LeadPlanner planner, Map.Entry<UUID, LeadResponse>... expectedResult) {
        Map<UUID, LeadResponse> result = planner.plan();

        Assert.assertEquals(expectedResult.length, result.size());
        for (Map.Entry<UUID, LeadResponse> entry : expectedResult) {
            LeadResponse expectedResponse = entry.getValue();
            LeadResponse response = result.get(entry.getKey());

            Assert.assertEquals(expectedResponse.getAlreadyProcessedIds(), response.getAlreadyProcessedIds());
            Assert.assertEquals(expectedResponse.getToCommitIds(), response.getToCommitIds());
        }
    }

    private static void notifyTxCommitted(LeadPlanner planner, UUID consumerId, long... txIds) {
        planner.markCommitted(consumerId, ids(txIds));
    }

    private static Map.Entry<UUID, LeadResponse> response(UUID nodeId, TLongList toCommitIds, TLongList toRemoveIds) {
        return new AbstractMap.SimpleImmutableEntry<>(nodeId, new LeadResponse(toCommitIds, toRemoveIds));
    }

    private static Map.Entry<UUID, LeadResponse> response(UUID nodeId, long... txIds) {
        return response(nodeId, ids(txIds), null);
    }

    private static IgniteBiTuple<String, List> scope(String cacheName, Object... keys) {
        List keysList = Arrays.asList(keys);
        return new IgniteBiTuple<>(cacheName, Collections.unmodifiableList(keysList));
    }

    @SafeVarargs
    private static TxInfo txInfo(UUID consumerId, long txId, IgniteBiTuple<String, List>... cacheScopes) {
        TransactionMetadata txMetadata = new TransactionMetadata(txId, Arrays.asList(cacheScopes));
        return new TxInfo(consumerId, txMetadata);
    }

    private static TLongList ids(long... txIds) {
        return new TLongArrayList(txIds);
    }

    private static class SimpleReference<T> implements Reference<T> {
        private T value;

        @Override
        public T get() {
            return value;
        }

        @Override
        public void set(T value) {
            this.value = value;
        }

        @Override
        public T initIfAbsent(T value) {
            if (this.value == null) {
                set(value);
            }
            return value;
        }
    }
}
