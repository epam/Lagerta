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

package com.epam.lagerta.subscriber.lead;

import com.epam.lagerta.capturer.TransactionScope;
import com.epam.lagerta.subscriber.ReaderTxScope;
import com.epam.lagerta.subscriber.util.MergeUtil;

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
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.epam.lagerta.subscriber.util.MergeUtil.getNext;

public class ReadTransactions implements Iterable<ReaderTxScope> {
    private static final Comparator<ReaderTxScope> SCOPE_COMPARATOR =
            Comparator.comparingLong(ReaderTxScope::getTransactionId);
    private static final long INITIAL_READ_ID = -2L;
    private static final long INITIAL_READY_READ_ID = -1L;
    private static final int INITIAL_CAPACITY = 100;

    // Reader availability levels.
    private static final int UNKNOWN = 0;
    private static final int DEAD = 1;
    private static final int LOST = 2;
    private static final int ALIVE = 3;

    private final List<ReaderTxScope> scopes = new LinkedList<>();
    private long lastDenseRead;

    private List<List<ReaderTxScope>> buffer;
    private boolean duplicatesPruningScheduled = false;

    /**
     * creates not ready process transactions
     */
    public ReadTransactions() {
        lastDenseRead = INITIAL_READ_ID;
        buffer = new ArrayList<>(INITIAL_CAPACITY);
    }

    @Override
    public Iterator<ReaderTxScope> iterator() {
        Spliterator<ReaderTxScope> spliterator = Spliterators.spliteratorUnknownSize(
                scopes.iterator(), Spliterator.SORTED | Spliterator.ORDERED);
        return StreamSupport
                .stream(spliterator, false)
                .filter(tx -> tx.getTransactionId() <= lastDenseRead)
                .distinct()
                .iterator();
    }

    /**
     * Makes {@code ReadTransactions} ready to process transactions.
     * After this method must be called {@link #pruneCommitted}
     */
    public void makeReady() {
        if (lastDenseRead == INITIAL_READ_ID) {
            lastDenseRead = INITIAL_READY_READ_ID;
        }
    }

    public long getLastDenseRead() {
        return lastDenseRead;
    }

    public void addAllOnNode(UUID readerId, List<TransactionScope> scopes) {
        List<ReaderTxScope> collect = scopes.stream()
                .map(tx -> new ReaderTxScope(readerId, tx.getTransactionId(), tx.getScope()))
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
        Iterator<ReaderTxScope> iterator = scopes.iterator();
        long commit = committed.getLastDenseCommit();
        while (iterator.hasNext()) {
            if (iterator.next().getTransactionId() <= commit) {
                iterator.remove();
            } else {
                break;
            }
        }
    }

    public boolean isProbableGap(long txId) {
        return !scopes.isEmpty() && txId == getLastDenseRead();
    }

    public List<Long> gapsInSparseTransactions() {
        List<LongStream> result = new ArrayList<>();
        Stream.concat(Stream.of(getLastDenseRead()), scopes.stream().map(TransactionScope::getTransactionId))
                .filter(txId -> txId >= getLastDenseRead())
                .reduce((left, right) -> {
                    if (right - left > 1) {
                        result.add(LongStream.range(left + 1, right));
                    }
                    return right;
                });
        return result.stream().flatMap(LongStream::boxed).collect(Collectors.toList());
    }

    private void compress(Heartbeats heartbeats, Set<UUID> lostReaders, Set<Long> inProgress) {
        mergeCollections(heartbeats, lostReaders, inProgress);
        for (ReaderTxScope next : scopes) {
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
        List<ReaderTxScope> mergedBuffer = MergeUtil.mergeBuffer(buffer, SCOPE_COMPARATOR);

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
                        .filter(scope -> diedReaders.contains(scope.getReaderId()))
                        .forEach(ReaderTxScope::markOrphan);
            }
            if (someoneDied || duplicatesPruningScheduled) {
                Consumer<ReaderTxScope> onRemove = tx -> {
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

    private static Set<UUID> mergeWithDeduplication(List<ReaderTxScope> scopes, List<ReaderTxScope> mergedBuffer,
                                                    Comparator<ReaderTxScope> comparator, Set<UUID> lostReaders) {
        Set<UUID> diedReaders = new HashSet<>();
        ListIterator<ReaderTxScope> firstIter = scopes.listIterator();
        Iterator<ReaderTxScope> secondIter = mergedBuffer.iterator();
        ReaderTxScope a = getNext(firstIter);
        ReaderTxScope b = getNext(secondIter);

        while (a != null && b != null) {
            int cmp = comparator.compare(a, b);

            if (cmp > 0) {
                firstIter.previous();
                firstIter.add(b);
                firstIter.next();
                b = getNext(secondIter);
            } else if (cmp < 0) {
                a = getNext(firstIter);
            } else if (lostReaders.contains(a.getReaderId())) {
                diedReaders.add(a.getReaderId());
                a = getNext(firstIter);
            }
        }
        while (b != null) {
            firstIter.add(b);
            b = getNext(secondIter);
        }
        return diedReaders;
    }

    private static void deduplicate(Set<UUID> lostReaders, List<ReaderTxScope> transactions,
                                    Consumer<ReaderTxScope> onRemove) {
        if (transactions.isEmpty()) {
            return;
        }
        int level = UNKNOWN;
        int count = 0;
        long id = INITIAL_READ_ID;
        for (ListIterator<ReaderTxScope> it = transactions.listIterator(); it.hasNext(); ) {
            ReaderTxScope tx = it.next();
            long thisId = tx.getTransactionId();
            if (thisId > id) {
                id = thisId;
                level = UNKNOWN;
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
                        ReaderTxScope previous = it.previous();
                        it.remove();
                        onRemove.accept(previous);
                        count--;
                    }
                    it.next();
                }
                count++;
            }
        }
    }

    private static int getLevel(ReaderTxScope scope, Set<UUID> lostReaders) {
        return scope.isOrphan()
                ? DEAD : lostReaders.contains(scope.getReaderId())
                            ? LOST
                            : ALIVE;
    }
}
