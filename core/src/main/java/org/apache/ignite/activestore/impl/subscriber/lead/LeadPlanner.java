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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.ignite.activestore.commons.Lazy;
import org.apache.ignite.activestore.impl.transactions.JointTxScope;
import org.apache.ignite.lang.IgniteClosure;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

/**
 * @author Evgeniy_Ignatiev
 * @since 19:33 11/30/2016
 */
class LeadPlanner {
    private static final IgniteClosure<UUID, JointTxScope> TX_SCOPE = new IgniteClosure<UUID, JointTxScope>() {
        @Override public JointTxScope apply(UUID uuid) {
            return new JointTxScope();
        }
    };

    private static final IgniteClosure<UUID, MutableLongList> ARRAY_LIST = new IgniteClosure<UUID, MutableLongList>() {
        @Override
        public MutableLongList apply(UUID consumerId) {
            return new LongArrayList();
        }
    };

    private final LeadPlanningState state;

    public LeadPlanner(LeadPlanningState state) {
        this.state = state;
    }

    public void registerNew(List<TxInfo> newTxInfos) {
        filterCommitted(newTxInfos);
        MergeHelper.mergeTo(newTxInfos, state.txInfos());
        for (TxInfo newTxInfo : newTxInfos) {
            long id = newTxInfo.id();

            state.orphanTransactions().remove(id);
            state.inProgressTransactions().remove(id);
        }
        // Update last dense read.
        long next = state.lastDenseRead() + 1;
        for (TxInfo txInfo : state.txInfos()) {
            long id = txInfo.id();
            if (id == next) {
                next++;
            }
            else if (id > next) {
                break;
            }
        }
        state.setLastDenseRead(next - 1);
    }

    public Map<UUID, LeadResponse> plan() {
        Lazy<UUID, MutableLongList> toCommit = findToCommitPerNode();
        Lazy<UUID, MutableLongList> toRemove = takeToRemoveAsLazyMap();
        Map<UUID, LeadResponse> result = new HashMap<>(toCommit.keySet().size());

        for (Map.Entry<UUID, MutableLongList> entry : toCommit) {
            LongList toRemoveList = toRemove.containsKey(entry.getKey()) ? toRemove.get(entry.getKey()) : null;
            result.put(entry.getKey(), new LeadResponse(entry.getValue(), toRemoveList));
        }
        for (Map.Entry<UUID, MutableLongList> entry : toRemove) {
            if (!result.containsKey(entry.getKey())) {
                result.put(entry.getKey(), new LeadResponse(null, entry.getValue()));
            }
        }
        return result;
    }

    private Lazy<UUID, MutableLongList> findToCommitPerNode() {
        JointTxScope globallyBlockedScope = new JointTxScope();
        Lazy<UUID, JointTxScope> ownedScopePerNode = new Lazy<>(TX_SCOPE);
        Lazy<UUID, MutableLongList> toCommit = new Lazy<>(ARRAY_LIST);

        for (TxInfo txInfo : state.txInfos()) {
            long id = txInfo.id();

            if (id > state.lastDenseRead()) {
                break;
            }
            else if (state.orphanTransactions().contains(id) || state.inProgressTransactions().contains(id)) {
                globallyBlockedScope.addAll(txInfo.scopeIterator());
            }
            else {
                boolean globallyAvailable = !globallyBlockedScope.intersects(txInfo.scopeIterator());
                if (globallyAvailable && isTxAvailableToCommitOnOwnNode(txInfo, ownedScopePerNode)) {
                    toCommit.get(txInfo.consumerId()).add(id);
                    ownedScopePerNode.get(txInfo.consumerId()).addAll(txInfo.scopeIterator());
                }
                else {
                    globallyBlockedScope.addAll(txInfo.scopeIterator());
                }
            }
        }
        return toCommit;
    }

    private Lazy<UUID, MutableLongList> takeToRemoveAsLazyMap() {
        Lazy<UUID, MutableLongList> toRemoveLazyMap = new Lazy<>(ARRAY_LIST);

        for (TxInfo txInfo : state.toRemove()) {
            toRemoveLazyMap.get(txInfo.consumerId()).add(txInfo.id());
        }
        state.toRemove().clear();
        return toRemoveLazyMap;
    }

    public void markCommitted(UUID consumerId, LongList txIds) {
        LongIterator txIdIt = txIds.longIterator();
        ListIterator<TxInfo> txInfoIterator = state.txInfoListIterator();
        long currentTxId = txIdIt.next(); // Should always be at least one value.

        // As in case of failover lead can get metadata of currently waiting transactions from consumers
        // only through the regular poll-apply cycle and transaction can be committed asynchronously
        // there is possibility to receive notification on committed transactions before receiving
        // metadata about currently available transaction on consumer.
        while (currentTxId != -1 && txInfoIterator.hasNext()) {
            TxInfo txInfo = txInfoIterator.next();

            if (currentTxId == txInfo.id()) {
                txInfoIterator.remove();
                if (consumerId.equals(txInfo.consumerId())) {
                    state.inProgressTransactions().remove(currentTxId);
                }
                else if (!state.inProgressTransactions().contains(currentTxId)) {
                    state.toRemove().add(txInfo);
                }
            }
            else if (currentTxId < txInfo.id()) {
                currentTxId = txIdIt.hasNext() ? txIdIt.next() : -1;
                txInfoIterator.previous();
            }
        }
        long lastDenseCommitted = MergeHelper.mergeWithDenseCompaction(txIds, state.sparseCommitted(),
            state.lastDenseCommitted());
        state.setLastDenseCommitted(lastDenseCommitted);
    }

    private boolean isTxAvailableToCommitOnOwnNode(TxInfo txInfo, Lazy<UUID, JointTxScope> ownedScopePerNode) {
        JointTxScope sameNodeScope = ownedScopePerNode.get(txInfo.consumerId());

        if (sameNodeScope.containsAll(txInfo.scopeIterator())) {
            return true;
        }
        for (Map.Entry<UUID, JointTxScope> entry : ownedScopePerNode) {
            UUID nodeId = entry.getKey();
            JointTxScope nodeScope = entry.getValue();

            if (!nodeId.equals(txInfo.consumerId()) && nodeScope.intersects(txInfo.scopeIterator())) {
                return false;
            }
        }
        return true;
    }

    public void markInProgressTransactions(LongList txIds) {
        for (LongIterator it = txIds.longIterator(); it.hasNext(); ) {
            state.inProgressTransactions().add(it.next());
        }
    }

    public boolean reconciliationFinishedConditionMet() {
        return state.lastDenseCommitted() >= state.gapUpperBound();
    }

    public void registerOutOfOrderConsumer(UUID consumerId) {
        for (TxInfo txInfo : state.txInfos()) {
            if (txInfo.consumerId().equals(consumerId)) {
                state.orphanTransactions().add(txInfo.id());
            }
        }
    }

    public void reuniteConsumer(UUID consumerId) {
        for (TxInfo txInfo : state.txInfos()) {
            if (txInfo.consumerId().equals(consumerId)) {
                state.orphanTransactions().remove(txInfo.id());
            }
        }
    }

    public Set<UUID> getFinallyDeadConsumers(Set<UUID> consumerOutOfOrder) {
        Set<UUID> result = new HashSet<>(consumerOutOfOrder);
        for (TxInfo txInfo : state.txInfos()) {
            if (state.orphanTransactions().contains(txInfo.id())) {
                //this consumer could be alive
                result.remove(txInfo.consumerId());
            }
        }
        return result;
    }

    public void updateContext(long loadedLastDenseCommitted, LongList loadedSparseCommitted) {
        long lastDenseCommitted = MergeHelper.mergeWithDenseCompaction(loadedSparseCommitted, state.sparseCommitted(),
            loadedLastDenseCommitted);
        state.setLastDenseCommitted(lastDenseCommitted);
        filterCommitted(state.txInfos());
        filterCommitted(state.inProgressTransactions());

        // Update last dense read.
        long next = state.lastDenseCommitted() + 1;
        ListIterator<TxInfo> txInfoIt = state.txInfoListIterator();

        for (LongIterator sparseIt = state.sparseCommitted().longIterator(); sparseIt.hasNext(); ) {
            long sparseId = sparseIt.next();

            if (sparseId > next) {
                while (txInfoIt.hasNext()) {
                    long id = txInfoIt.next().id();

                    if (next == id) {
                        next++;
                    }
                    else if (id > next) {
                        txInfoIt.previous();
                        break;
                    }
                }
            }
            if (sparseId == next) {
                next++;
            }
            else if (sparseId > next) {
                break;
            }
        }
        while (txInfoIt.hasNext()) {
            long id = txInfoIt.next().id();

            if (next == id) {
                next++;
            }
            else if (id > next) {
                txInfoIt.previous();
                break;
            }
        }
        state.setLastDenseRead(next - 1);
    }

    private void filterCommitted(List<TxInfo> infos) {
        for (Iterator<TxInfo> it = infos.iterator(); it.hasNext(); ) {
            TxInfo txInfo = it.next();

            if (txInfo.id() <= state.lastDenseCommitted() || state.sparseCommitted().contains(txInfo.id())) {
                it.remove();
                state.toRemove().add(txInfo);
            }
        }
    }

    private void filterCommitted(MutableLongSet ids) {
        for (MutableLongIterator it = ids.longIterator(); it.hasNext(); ) {
            long id = it.next();
            if (id <= state.lastDenseCommitted() || state.sparseCommitted().contains(id)) {
                it.remove();
            }
        }
    }

}
