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

    private static final long INITIAL_READ_ID = -1L;
    private static final int INITIAL_CAPACITY = 100;

    private final List<ConsumerTxScope> allTransactions = new LinkedList<>();
    private long lastDenseRead = INITIAL_READ_ID;
    private List<List<ConsumerTxScope>> buffer = new ArrayList<>(INITIAL_CAPACITY);

    public long getLastDenseRead() {
        return lastDenseRead;
    }

    public void pruneCommitted(CommittedTransactions committed) {
        compress();
        Iterator<ConsumerTxScope> iterator = allTransactions.iterator();
        while (iterator.hasNext()) {
            long commit = committed.getLastDenseCommit();
            if (iterator.next().getTransactionId() <= commit) {
                iterator.remove();
            } else {
                break;
            }
        }
    }

    public void addAllOnNode(UUID consumerId, List<TransactionScope> txScopes) {
        List<ConsumerTxScope> collect = txScopes.stream()
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

    private void compress() {
        mergeCollections();
        for (ConsumerTxScope next : allTransactions) {
            if (lastDenseRead + 1 == next.getTransactionId()) {
                lastDenseRead = next.getTransactionId();
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
