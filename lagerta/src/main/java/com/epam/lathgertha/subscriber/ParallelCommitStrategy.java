package com.epam.lathgertha.subscriber;

import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.lang.IgniteFuture;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implemented parallel logic to commit batch of transactions.
 */
public class ParallelCommitStrategy implements CommitStrategy {
    private static final int POOL_COUNT = 5;

    private final CommitServitor commitServitor;
    private final IgniteScheduler scheduler;

    public ParallelCommitStrategy(CommitServitor commitServitor, IgniteScheduler scheduler) {
        this.commitServitor = commitServitor;
        this.scheduler = scheduler;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Long> commit(List<Long> txIdsToCommit, Map<Long, TransactionData> buffer) {
        return new ParallelExecutor(buffer).commit(txIdsToCommit);
    }

    /**
     * Stores state of commit of single batch
     */
    private class ParallelExecutor {
        private final Map<Long, TransactionData> buffer;

        private final Map<Map.Entry<String, Object>, Long> lastOwner = new HashMap<>();
        private final Map<Long, TransactionRelation> relationMap = new HashMap<>();
        private final BlockingQueue<TransactionRelation> tasks = new LinkedBlockingQueue<>();
        private final AtomicInteger count = new AtomicInteger();
        private volatile boolean deadHasRisen = false;

        ParallelExecutor(Map<Long, TransactionData> buffer) {
            this.buffer = buffer;
        }

        @SuppressWarnings("unchecked")
        public List<Long> commit(List<Long> txIdsToCommit) {
            txIdsToCommit
                    .stream()
                    .map(txId -> relation(txId, buffer))
                    .peek(relation -> relationMap.put(relation.id, relation))
                    .filter(TransactionRelation::isFree)
                    .forEach(tasks::add);

            relationMap.values().forEach(relation -> relation.fillRelations(relationMap));

            count.set(relationMap.size());
            IntStream
                    .range(0, Math.min(POOL_COUNT, relationMap.size()))
                    .boxed()
                    .map(i -> (Runnable) this::execute)
                    .map(scheduler::runLocal)
                    .collect(Collectors.toList())
                    .forEach(IgniteFuture::get);

            return deadHasRisen
                    ? txIdsToCommit.stream()
                    .map(relationMap::get)
                    .filter(TransactionRelation::isAlive)
                    .map(TransactionRelation::getId)
                    .collect(Collectors.toList())
                    : txIdsToCommit;
        }

        private TransactionRelation relation(Long txId, Map<Long, TransactionData> buffer) {
            return new TransactionRelation(txId, buffer
                    .get(txId)
                    .getTransactionScope()
                    .getScope()
                    .stream()
                    .flatMap(cacheKeys -> ((Stream<?>) cacheKeys.getValue()
                            .stream())
                            .map(key -> compositeKey(cacheKeys.getKey(), key)))
                    .map(compositeKey -> lastOwner.put(compositeKey, txId))
                    .distinct()
                    .filter(Objects::nonNull)
                    .map(relationMap::get)
                    .peek(blocker -> blocker.addDependent(txId))
                    .count());
        }

        private void execute() {
            try {
                while (count.getAndDecrement() > 0) {
                    TransactionRelation relation = tasks.take();
                    boolean alive = relation.isAlive();
                    if (alive) {
                        if (commitServitor.commit(relation.getId(), buffer)) {
                            relation
                                    .dependent()
                                    .stream()
                                    .filter(TransactionRelation::release)
                                    .forEach(tasks::add);
                            continue;
                        }
                        deadHasRisen = true;
                        relation.kill();
                    }
                    relation
                            .dependent()
                            .stream()
                            .peek(TransactionRelation::kill)
                            .forEach(tasks::add);
                }
            } catch (InterruptedException | IgniteInterruptedException e) {
                //do nothing
            }
        }

        private Map.Entry<String, Object> compositeKey(String cacheName, Object o) {
            return new AbstractMap.SimpleImmutableEntry<>(cacheName, o);
        }
    }

    /**
     * Stores single transaction relations
     */
    private static class TransactionRelation {
        private final long id;
        private AtomicInteger count;
        private final List<Long> dependentIds;
        private final List<TransactionRelation> dependent;
        private volatile boolean alive;

        TransactionRelation(long id, long count) {
            this.id = id;
            this.count = new AtomicInteger((int) count);
            dependentIds = new ArrayList<>();
            dependent = new ArrayList<>();
            alive = true;
        }

        long getId() {
            return id;
        }

        List<TransactionRelation> dependent() {
            return dependent;
        }

        void addDependent(long id) {
            dependentIds.add(id);
        }

        void fillRelations(Map<Long, TransactionRelation> relationMap) {
            dependentIds.stream().map(relationMap::get).forEach(dependent::add);
        }

        boolean release() {
            return count.decrementAndGet() == 0;
        }

        boolean isFree() {
            return count.get() == 0;
        }

        void kill() {
            alive = false;
        }

        public boolean isAlive() {
            return alive;
        }
    }
}
