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
    private final Set<Long> deadTransactions = new HashSet<>();

    private long lastDenseRead = INITIAL_READ_ID;
    private List<List<ConsumerTxScope>> buffer = new ArrayList<>(INITIAL_CAPACITY);
    private boolean duplicatesPruningScheduled = false;

    public long getLastDenseRead() {
        return lastDenseRead;
    }

    public void pruneCommitted(CommittedTransactions committed, Set<UUID> lostReaders) {
        compress(lostReaders);
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

    public boolean isDead(long transactionId) {
        return deadTransactions.contains(transactionId);
    }

    public void scheduleDuplicatesPrunning() {
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
        // ToDo: Ensure alive will appear before dead.
        // Simple solution: tweak MergeUtil#merge to add new duplicates before old ones.
        // So with an old tx arrived from alive reader we will encounter before any that were
        // marked dead previously, as there can be a case when a tx was marked dead, but no
        // changes in reader topology happened after, yet another readers have taken this tx.
        return StreamSupport
            .stream(spliterator, false)
            .filter(tx -> tx.getTransactionId() <= lastDenseRead)
            .distinct()
            .iterator();
    }

    private void compress(Set<UUID> lostReaders) {
        mergeCollections(lostReaders);
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

    private void mergeCollections(Set<UUID> lostReaders) {
        List<ConsumerTxScope> mergedBuffer = MergeUtil.mergeBuffer(buffer, SCOPE_COMPARATOR);

        if (duplicatesPruningScheduled) {
            Set<UUID> dieadReaders = mergeWithDeduplication(lostReaders, mergedBuffer);

            allTransactions.removeIf(scope -> dieadReaders.contains(scope.getConsumerId()));
            duplicatesPruningScheduled = false;
        } else {
            MergeUtil.merge(allTransactions, mergedBuffer, SCOPE_COMPARATOR);
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
                deadTransactions.add(a.getTransactionId());
                firstIter.remove();
                firstIter.next();
            }
        }
        while (b != null) {
            firstIter.add(b);
            b = MergeUtil.getNext(secondIter);
        }
        return diedReaders;
    }
}
