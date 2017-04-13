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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.epam.lathgertha.subscriber.util.MergeUtil.getNext;

public class ReadTransactions implements Iterable<ConsumerTxScope> {
    private static final Comparator<ConsumerTxScope> SCOPE_COMPARATOR =
            Comparator.comparingLong(ConsumerTxScope::getTransactionId);
    private static final long INITIAL_READ_ID = -1L;
    private static final int INITIAL_CAPACITY = 100;

    private final List<ConsumerTxScope> scopes = new LinkedList<>();
    private long lastDenseRead;

    private List<List<ConsumerTxScope>> buffer;
    private boolean duplicatesPruningScheduled = false;

    /**
     * creates not ready process transactions
     */
    public ReadTransactions() {
        lastDenseRead = INITIAL_READ_ID;
        buffer = new ArrayList<>(INITIAL_CAPACITY);
    }

    @Override
    public Iterator<ConsumerTxScope> iterator() {
        Spliterator<ConsumerTxScope> spliterator = Spliterators.spliteratorUnknownSize(
                scopes.iterator(), Spliterator.SORTED | Spliterator.ORDERED);
        return StreamSupport
                .stream(spliterator, false)
                .filter(tx -> tx.getTransactionId() <= lastDenseRead)
                .distinct()
                .iterator();
    }

    public long getLastDenseRead() {
        return lastDenseRead;
    }

    public void addAllOnNode(UUID readerId, List<TransactionScope> scopes) {
        List<ConsumerTxScope> collect = scopes.stream()
                .map(tx -> new ConsumerTxScope(readerId, tx.getTransactionId(), tx.getScope()))
                .collect(Collectors.toList());
        buffer.add(collect);
    }

    public void scheduleDuplicatesPruning() {
        duplicatesPruningScheduled = true;
    }

    public void pruneCommitted(
            CommittedTransactions committed,
            Heartbeats heartbeats,
            Set<UUID> lostReaders,
            Set<Long> inProgress
    ) {
        compress(heartbeats, lostReaders, inProgress);
        Iterator<ConsumerTxScope> iterator = scopes.iterator();
        long commit = committed.getLastDenseCommit();
        while (iterator.hasNext()) {
            if (iterator.next().getTransactionId() <= commit) {
                iterator.remove();
            } else {
                break;
            }
        }
    }

    private void compress(Heartbeats heartbeats, Set<UUID> lostReaders, Set<Long> inProgress) {
        mergeCollections(heartbeats, lostReaders, inProgress);
        for (ConsumerTxScope next : scopes) {
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

        if (!duplicatesPruningScheduled && lostReaders.isEmpty()) {
            MergeUtil.merge(scopes, mergedBuffer, SCOPE_COMPARATOR);
        } else {
            Set<UUID> diedReaders = mergeWithDeduplication(scopes, mergedBuffer, SCOPE_COMPARATOR, lostReaders);
            boolean someoneDied = !diedReaders.isEmpty();

            if (someoneDied) {
                diedReaders
                        .stream()
                        .peek(lostReaders::remove)
                        .forEach(heartbeats::removeDead);
                scopes
                        .stream()
                        .filter(scope -> diedReaders.contains(scope.getConsumerId()))
                        .forEach(ConsumerTxScope::markOrphan);
            }
            if (someoneDied || duplicatesPruningScheduled) {
                Consumer<ConsumerTxScope> onRemove = tx -> {
                    if (tx.isInProgress() && tx.isOrphan()) {
                        inProgress.remove(tx.getTransactionId());
                    }
                };
                deduplicate(lostReaders, scopes, onRemove);
                duplicatesPruningScheduled = false;
            }
        }
        buffer = new ArrayList<>(INITIAL_CAPACITY);
    }

    private static Set<UUID> mergeWithDeduplication(List<ConsumerTxScope> scopes, List<ConsumerTxScope> mergedBuffer,
                                                    Comparator<ConsumerTxScope> comparator, Set<UUID> lostReaders) {
        Set<UUID> diedReaders = new HashSet<>();
        ListIterator<ConsumerTxScope> firstIter = scopes.listIterator();
        Iterator<ConsumerTxScope> secondIter = mergedBuffer.iterator();
        ConsumerTxScope a = getNext(firstIter);
        ConsumerTxScope b = getNext(secondIter);

        while (a != null && b != null) {
            int cmp = comparator.compare(a, b);

            if (cmp > 0) {
                firstIter.previous();
                firstIter.add(b);
                firstIter.next();
                b = getNext(secondIter);
            } else if (cmp < 0) {
                a = getNext(firstIter);
            } else if (lostReaders.contains(a.getConsumerId())) {
                diedReaders.add(a.getConsumerId());
                a = getNext(firstIter);
            }
        }
        while (b != null) {
            firstIter.add(b);
            b = getNext(secondIter);
        }
        return diedReaders;
    }

    private static void deduplicate(Set<UUID> lostReaders, List<ConsumerTxScope> transactions,
                                    Consumer<ConsumerTxScope> onRemove) {
        if (transactions.isEmpty()) {
            return;
        }
        int level = 0;          //0 - Unknown, 1 - Dead, 2 - Lost, 3 - Alive
        int count = 0;
        long id = INITIAL_READ_ID;
        for (ListIterator<ConsumerTxScope> it = transactions.listIterator(); it.hasNext(); ) {
            ConsumerTxScope tx = it.next();
            long thisId = tx.getTransactionId();
            if (thisId > id) {
                id = thisId;
                level = 0;
                count = 0;
            }
            int thisLevel = getLevel(tx, lostReaders);
            if (thisLevel < level) {
                it.remove();
                onRemove.accept(tx);
            } else {
                if (thisLevel > level) {
                    level = thisLevel;
                    it.previous();
                    while (count > 0) {
                        it.previous();
                        it.remove();
                        onRemove.accept(tx);
                        count--;
                    }
                    it.next();
                }
                count++;
            }
        }
    }

    private static int getLevel(ConsumerTxScope scope, Set<UUID> lostReaders) {
        return scope.isOrphan()
                ? 1
                : lostReaders.contains(scope.getConsumerId())
                ? 2
                : 3;
    }
}
