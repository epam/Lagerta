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

            pruneDuplicates(heartbeats, diedReaders, lostReaders, inProgress);
        }
        buffer = new ArrayList<>(INITIAL_CAPACITY);
    }

    private static void deduplicate(Set<UUID> lostReaders, List<ConsumerTxScope> transactions) {
        if (transactions.isEmpty()) {
            return;
        }
        int level = 0;          //0 - Unknown, 1 - Dead, 2 - Lost, 3 - Alive
        int count = 0;
        long id = -2L;
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
            } else {
                if (thisLevel > level) {
                    level = thisLevel;
                    while (count > 0) {
                        it.previous();
                        it.remove();
                        count--;
                    }
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

    private void pruneDuplicates(
            Heartbeats heartbeats,
            Set<UUID> diedReaders,
            Set<UUID> lostReaders,
            Set<Long> inProgress
    ) {
        if (allTransactions.isEmpty()) {
            return;
        }
        ListIterator<ConsumerTxScope> it = allTransactions.listIterator();
        int encounteredLost = 0;
        int encounteredDead = 0;
        boolean aliveEncountered = false;
        long txId = it.next().getTransactionId();

        it.previous();
        while (it.hasNext()) {
            ConsumerTxScope scope = it.next();
            long scopeTxId = scope.getTransactionId();
            UUID consumerId = scope.getConsumerId();

            if (scopeTxId != txId) {
                if (encounteredDead != 0) {
                    orphanTransactions.add(txId);
                    inProgress.remove(txId);
                }
                encounteredLost = 0;
                encounteredDead = 0;
                aliveEncountered = false;
                txId = scopeTxId;
            }
            boolean isAlive = !lostReaders.contains(consumerId) && !diedReaders.contains(consumerId);
            boolean isLost = lostReaders.contains(consumerId) && !diedReaders.contains(consumerId);

            if (isAlive) {
                if (!aliveEncountered) {
                    aliveEncountered = true;
                    orphanTransactions.remove(txId);
                    if (encounteredLost > 0 || encounteredDead > 0) {
                        removePreviousTxs(it, encounteredDead + encounteredLost, txScope -> {
                            UUID previousId = txScope.getConsumerId();

                            if (lostReaders.contains(previousId)) {
                                diedReaders.add(previousId);
                                lostReaders.remove(previousId);
                                heartbeats.removeDead(previousId);
                            }
                        });
                        encounteredLost = 0;
                        encounteredDead = 0;
                    }
                }
            } else if (isLost) {
                if (encounteredDead > 0) {
                    orphanTransactions.remove(txId);
                    removePreviousTxs(it, encounteredDead, txScope -> {});
                    encounteredDead = 0;
                } else if (aliveEncountered) {
                    diedReaders.add(consumerId);
                    lostReaders.remove(consumerId);
                    heartbeats.removeDead(consumerId);
                    it.remove();
                } else {
                    encounteredLost++;
                }
            } else {
                if (encounteredLost > 0 || aliveEncountered) {
                    it.remove();
                } else {
                    encounteredDead++;
                }
                diedReaders.add(consumerId);
            }
        }
    }

    private static void removePreviousTxs(
        ListIterator<ConsumerTxScope> it,
        int number,
        Consumer<ConsumerTxScope> removedTxsProcessor
    ) {
        it.previous(); // To not remove current element;
        for (int i = 0; i < number; i++) {
            ConsumerTxScope scope = it.previous();

            it.remove();
            removedTxsProcessor.accept(scope);
        }
        it.next();
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
