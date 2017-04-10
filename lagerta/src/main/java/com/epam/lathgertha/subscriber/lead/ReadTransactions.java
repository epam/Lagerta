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

package com.epam.lathgertha.subscriber.lead;

import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.subscriber.ConsumerTxScope;
import com.epam.lathgertha.subscriber.util.MergeUtil;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class ReadTransactions implements Iterable<ConsumerTxScope> {

    private static final long INITIAL_READY_READ_ID = -1L;
    private static final long INITIAL_READ_ID = -2L;
    private static final int INITIAL_CAPACITY = 100;

    private final List<ConsumerTxScope> allTransactions = new LinkedList<>();
    private long lastDenseRead;
    private List<List<ConsumerTxScope>> buffer;

    /**
     * creates not ready process transactions
     */
    public ReadTransactions() {
        lastDenseRead = INITIAL_READ_ID;
        buffer = new ArrayList<>(INITIAL_CAPACITY);
    }

    public long getLastDenseRead() {
        return lastDenseRead;
    }

    public void pruneCommitted(CommittedTransactions committed) {
        compress();
        Iterator<ConsumerTxScope> iterator = allTransactions.iterator();
        long commit = committed.getLastDenseCommit();
        while (iterator.hasNext()) {
            if (iterator.next().getTransactionId() <= commit) {
                iterator.remove();
            } else {
                break;
            }
        }
    }

    public void addAllOnNode(UUID consumerId, List<TransactionScope> scopes) {
        List<ConsumerTxScope> collect = scopes.stream()
                .map(tx -> new ConsumerTxScope(consumerId, tx.getTransactionId(), tx.getScope()))
                .collect(Collectors.toList());
        buffer.add(collect);
    }

    @Override
    public Iterator<ConsumerTxScope> iterator() {
        return allTransactions.stream()
                .filter(tx -> tx.getTransactionId() <= lastDenseRead)
                .iterator();
    }

    /**
     * makes this ready to process transactions
     */
    public void setReady() {
        if (lastDenseRead == INITIAL_READ_ID) {
            lastDenseRead = INITIAL_READY_READ_ID;
            compress();
        }
    }

    private void compress() {
        mergeCollections();
        for (ConsumerTxScope next : allTransactions) {
            long nextId = next.getTransactionId();
            if (lastDenseRead >= nextId) {
                // skip
            } else if (lastDenseRead + 1 == nextId) {
                lastDenseRead = nextId;
            } else {
                break;
            }
        }
    }

    private void mergeCollections() {
        MergeUtil.mergeCollections(
                allTransactions,
                buffer,
                Comparator.comparingLong(ConsumerTxScope::getTransactionId));
        buffer = new ArrayList<>(INITIAL_CAPACITY);
    }
}
