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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReadTransactions implements Iterable<ConsumerTxScope> {
    private static final Comparator<ConsumerTxScope> SCOPE_COMPARATOR =
        Comparator.comparingLong(ConsumerTxScope::getTransactionId);
    private static final long INITIAL_READ_ID = -1L;
    private static final int INITIAL_CAPACITY = 100;

    private final List<ConsumerTxScope> allTransactions = new LinkedList<>();
    private final Set<Long> orphanTransactions = new HashSet<>();

    private long lastDenseRead = INITIAL_READ_ID;
    private List<List<ConsumerTxScope>> buffer = new ArrayList<>(INITIAL_CAPACITY);
    private boolean duplicatesPruningScheduled = false;

    public long getLastDenseRead() {
        return lastDenseRead;
    }

    public void pruneCommitted(
            CommittedTransactions committed,
            Heartbeats heartbeats,
            Set<UUID> lostReaders,
            Set<Long> inProgress
    ) {
        compress(heartbeats, lostReaders, inProgress);
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

    public boolean isOrphan(long transactionId) {
        return orphanTransactions.contains(transactionId);
    }

    public void scheduleDuplicatesPruning() {
        if (!duplicatesPruningScheduled) {
            duplicatesPruningScheduled = true;
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
        Spliterator<ConsumerTxScope> spliterator = Spliterators.spliteratorUnknownSize(
            allTransactions.iterator(), Spliterator.SORTED | Spliterator.ORDERED);
        return StreamSupport
            .stream(spliterator, false)
            .filter(tx -> tx.getTransactionId() <= lastDenseRead)
            .distinct()
            .iterator();
    }

    private void compress(Heartbeats heartbeats, Set<UUID> lostReaders, Set<Long> inProgress) {
        mergeCollections(heartbeats, lostReaders, inProgress);
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

    private void mergeCollections(Heartbeats heartbeats, Set<UUID> lostReaders, Set<Long> inProgress) {
        List<ConsumerTxScope> mergedBuffer = MergeUtil.mergeBuffer(buffer, SCOPE_COMPARATOR);

        if (!duplicatesPruningScheduled && lostReaders.isEmpty() && orphanTransactions.isEmpty()) {
            MergeUtil.merge(allTransactions, mergedBuffer, SCOPE_COMPARATOR);
        } else {
            Set<UUID> diedReaders = mergeWithDeduplication(lostReaders, mergedBuffer);

            allTransactions
                    .stream()
                    .filter(scope -> diedReaders.contains(scope.getConsumerId()))
                    .map(ConsumerTxScope::getTransactionId)
                    .peek(inProgress::remove)
                    .forEach(orphanTransactions::add);
            diedReaders
                    .stream()
                    .peek(lostReaders::remove)
                    .forEach(heartbeats::removeDead);
        }
        buffer = new ArrayList<>(INITIAL_CAPACITY);
    }

    private Set<UUID> mergeWithDeduplication(Set<UUID> lostReaders, List<ConsumerTxScope> mergedBuffer) {
        Set<UUID> diedReaders = new HashSet<>();
        ListIterator<ConsumerTxScope> firstIter = allTransactions.listIterator();
        Iterator<ConsumerTxScope> secondIter = mergedBuffer.iterator();
        ConsumerTxScope a = MergeUtil.getNext(firstIter);
        ConsumerTxScope b = MergeUtil.getNext(secondIter);

        while (a != null && b != null) {
            int cmp = SCOPE_COMPARATOR.compare(a, b);

            if (cmp > 0) {
                firstIter.previous();
                firstIter.add(b);
                firstIter.next();
                b = MergeUtil.getNext(secondIter);
            } else if (cmp < 0) {
                a = MergeUtil.getNext(firstIter);
            } else if (lostReaders.contains(a.getConsumerId())) {
                diedReaders.add(a.getConsumerId());
                orphanTransactions.remove(a.getTransactionId());
                firstIter.remove();
                a = MergeUtil.getNext(firstIter);
            }
        }
        while (b != null) {
            firstIter.add(b);
            b = MergeUtil.getNext(secondIter);
        }
        return diedReaders;
    }
}
