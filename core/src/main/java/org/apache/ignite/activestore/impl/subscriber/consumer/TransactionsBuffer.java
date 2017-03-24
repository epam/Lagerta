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

package org.apache.ignite.activestore.impl.subscriber.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TLongList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.procedure.TLongObjectProcedure;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/27/2016 7:47 PM
 */
class TransactionsBuffer {
    private final TLongObjectMap<TransactionWrapper> buffer = new TLongObjectHashMap<>();
    private final Map<Long, TransactionWrapper> inProgressTransactions = new ConcurrentHashMap<>();
    private final Queue<List<TransactionWrapper>> committedTxs = new ConcurrentLinkedQueue<>();

    public Collection<TransactionWrapper> getUncommittedTxs() {
        return buffer.valueCollection();
    }

    public void add(TransactionWrapper transactionWrapper) {
        buffer.put(transactionWrapper.id(), transactionWrapper);
    }

    public TransactionWrapper getInProgressTx(long txId) {
        return inProgressTransactions.get(txId);
    }

    public void markInProgress(TLongList txIds) {
        for (TLongIterator it = txIds.iterator(); it.hasNext(); ) {
            long id = it.next();
            inProgressTransactions.put(id, buffer.get(id));
        }
    }

    public void markCommitted(TLongList txIds) {
        List<TransactionWrapper> transactions = new ArrayList<>(txIds.size());

        for (TLongIterator it = txIds.iterator(); it.hasNext(); ) {
            transactions.add(inProgressTransactions.remove(it.next()));
        }
        committedTxs.add(transactions);
    }

    public void markAlreadyCommitted(TLongList txIds) {
        List<TransactionWrapper> transactions = new ArrayList<>();

        for (TLongIterator it = txIds.iterator(); it.hasNext(); ) {
            TransactionWrapper wrapper = buffer.get(it.next());

            if (wrapper != null) {
                transactions.add(wrapper);
            }
            committedTxs.add(transactions);
        }
    }

    public List<List<TransactionWrapper>> getCommittedTxs() {
        int txSize = committedTxs.size();

        if (txSize == 0) {
            return Collections.emptyList();
        }
        List<List<TransactionWrapper>> allTransactions = new ArrayList<>(txSize);
        Iterator<List<TransactionWrapper>> it = committedTxs.iterator();

        for (int i = 0; i < txSize; i++) {
            allTransactions.add(it.next());
        }
        return allTransactions;
    }

    public void removeCommitted(List<List<TransactionWrapper>> committedTxsBatches) {
        for (List<TransactionWrapper> batch : committedTxsBatches) {
            committedTxs.remove();
            for (TransactionWrapper wrapper : batch) {
                buffer.remove(wrapper.id());
            }
        }
    }

    public void compact(long lastDenseCommittedId) {
        buffer.retainEntries(new CompactificationRetainer(lastDenseCommittedId, inProgressTransactions));
    }

    public int size() {
        return buffer.size();
    }

    private static class CompactificationRetainer implements TLongObjectProcedure<TransactionWrapper> {
        private final long lastDenseCommittedId;
        private final Map<Long, TransactionWrapper> inProgressTransactions;

        CompactificationRetainer(long lastDenseCommittedId, Map<Long, TransactionWrapper> inProgressTransactions) {
            this.lastDenseCommittedId = lastDenseCommittedId;
            this.inProgressTransactions = inProgressTransactions;
        }

        @Override public boolean execute(long txId, TransactionWrapper wrapper) {
            return txId > lastDenseCommittedId || inProgressTransactions.containsKey(txId);
        }
    }
}
