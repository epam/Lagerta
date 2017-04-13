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

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/27/2016 7:47 PM
 */
class TransactionsBuffer {
    private final MutableLongObjectMap<TransactionWrapper> buffer = new LongObjectHashMap<>();
    private final Map<Long, TransactionWrapper> inProgressTransactions = new ConcurrentHashMap<>();
    private final Queue<List<TransactionWrapper>> committedTxs = new ConcurrentLinkedQueue<>();

    public Collection<TransactionWrapper> getUncommittedTxs() {
        return buffer.values();
    }

    public void add(TransactionWrapper transactionWrapper) {
        buffer.put(transactionWrapper.id(), transactionWrapper);
    }

    public TransactionWrapper getInProgressTx(long txId) {
        return inProgressTransactions.get(txId);
    }

    public void markInProgress(LongList txIds) {
        for (LongIterator it = txIds.longIterator(); it.hasNext(); ) {
            long id = it.next();
            inProgressTransactions.put(id, buffer.get(id));
        }
    }

    public void markCommitted(LongList txIds) {
        List<TransactionWrapper> transactions = new ArrayList<>(txIds.size());

        for (LongIterator it = txIds.longIterator(); it.hasNext(); ) {
            transactions.add(inProgressTransactions.remove(it.next()));
        }
        committedTxs.add(transactions);
    }

    public void markAlreadyCommitted(LongList txIds) {
        List<TransactionWrapper> transactions = new ArrayList<>();

        for (LongIterator it = txIds.longIterator(); it.hasNext(); ) {
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
        for (MutableLongIterator it = buffer.keySet().longIterator(); it.hasNext(); ) {
            long txId = it.next();

            if (txId <= lastDenseCommittedId && !inProgressTransactions.containsKey(txId)) {
                it.remove();
            }
        }
    }

    public int size() {
        return buffer.size();
    }
}
