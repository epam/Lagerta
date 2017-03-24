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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.activestore.commons.Lazy;
import org.apache.ignite.lang.IgniteClosure;

/**
 * @author Evgeniy_Ignatiev
 * @since 19:33 11/30/2016
 */
public class LeadPlanner {
    private long lastContinuouslyReadTxId = -1;

    private final LinkedList<TxInfo> txInfos = new LinkedList<>();

    private final Set<Long> inProgressTransactions = new HashSet<>();

    void processTxsCommitted(List<Long> txIds) {
        ListIterator<TxInfo> txInfoIterator = txInfos.listIterator();

        for (long currentTxId : txIds) {
            while (currentTxId != txInfoIterator.next().id()) {
                // Always advance iterator without check for getting out of bounds
                // as received tx ids should always be present in it.
            }
            txInfoIterator.remove();
            inProgressTransactions.remove(currentTxId);
        }
    }

    void processTxsRead(List<TxInfo> newTxInfos) {
        mergeTo(newTxInfos, txInfos, new Comparator<TxInfo>() {
            @Override public int compare(TxInfo lhs, TxInfo rhs) {
                return Long.compare(lhs.id(), rhs.id());
            }
        });
        // Update last continuously read tx id.
        for (TxInfo txInfo : txInfos) {
            long diff = txInfo.id() - lastContinuouslyReadTxId;

            if (diff == 1) {
                lastContinuouslyReadTxId++;
            } else if (diff > 1) {
                return;
            }
        }
    }

    Map<UUID, List<Long>> getAvailableToCommitTransactions() {
        Set<Object> globallyBlockedScope = new HashSet<>();
        Lazy<UUID, Set<Object>> availableTxsPerNode = new Lazy<>(new IgniteClosure<UUID, Set<Object>>() {
            @Override public Set<Object> apply(UUID uuid) {
                return new HashSet<>();
            }
        });
        Lazy<UUID, List<Long>> result = new Lazy<>(new IgniteClosure<UUID, List<Long>>() {
            @Override public List<Long> apply(UUID consumerId) {
                return new ArrayList<>();
            }
        });
        for (TxInfo txInfo : txInfos) {
            if (txInfo.id() > lastContinuouslyReadTxId) {
                break;
            }
            if (inProgressTransactions.contains(txInfo.id())) {
                globallyBlockedScope.addAll(txInfo.scope());
            } else {
                boolean globallyAvailable = Collections.disjoint(globallyBlockedScope, txInfo.scope());

                if (globallyAvailable && isTxAvailableToCommitOnOwnNode(txInfo, availableTxsPerNode)) {
                    result.get(txInfo.consumerId()).add(txInfo.id());
                    availableTxsPerNode.get(txInfo.consumerId()).addAll(txInfo.scope());
                } else {
                    globallyBlockedScope.addAll(txInfo.scope());
                }
            }
        }
        return result.toMap();
    }

    private boolean isTxAvailableToCommitOnOwnNode(TxInfo txInfo, Lazy<UUID, Set<Object>> availableTxsPerNode) {
        Set<Object> sameNodeScope = availableTxsPerNode.get(txInfo.consumerId());

        if (sameNodeScope.containsAll(txInfo.scope())) {
            return true;
        }
        for (Map.Entry<UUID, Set<Object>> entry : availableTxsPerNode) {
            UUID nodeId = entry.getKey();
            Set<Object> nodeScope = entry.getValue();

            if (!nodeId.equals(txInfo.consumerId()) && !Collections.disjoint(nodeScope, txInfo.scope())) {
                return false;
            }
        }
        return true;
    }

    void markInProgressTransactions(List<Long> txIds) {
        inProgressTransactions.addAll(txIds);
    }

    private static <T> void mergeTo(List<T> fromList, LinkedList<T> toList, Comparator<T> comparator) {
        if (fromList.isEmpty()) {
            return;
        }
        Iterator<T> fromIterator = fromList.iterator();
        ListIterator<T> toIterator = toList.listIterator();
        T currentFromElement = null;

        if (toIterator.hasNext()) {
            currentFromElement = fromIterator.next();
            while (currentFromElement != null && toIterator.hasNext()) {
                if (comparator.compare(toIterator.next(), currentFromElement) > 0) {
                    toIterator.previous();
                    toIterator.add(currentFromElement);
                    currentFromElement = fromIterator.hasNext() ? fromIterator.next() : null;
                }
            }
        }
        if (currentFromElement != null) {
            toList.add(currentFromElement);
        }
        while (fromIterator.hasNext()) {
            toList.add(fromIterator.next());
        }
    }
}
