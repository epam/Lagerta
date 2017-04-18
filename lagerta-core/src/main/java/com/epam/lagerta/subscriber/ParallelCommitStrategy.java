package com.epam.lagerta.subscriber;

import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implemented parallel logic to commit batch of transactions.
 */
public class ParallelCommitStrategy implements CommitStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelCommitStrategy.class);
    private static final int POOL_COUNT = 5;

    private final CommitServitor commitServitor;
    private final ExecutorService executor;

    public ParallelCommitStrategy(CommitServitor commitServitor, String localGridName) {
        this.commitServitor = commitServitor;
        this.executor = Executors.newFixedThreadPool(POOL_COUNT, new IgniteThreadFactory(localGridName));
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
                    .map(executor::submit)
                    .collect(Collectors.toList())
                    .forEach(this::join);

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

        private void join(Future future) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e){
                LOGGER.error("[R] Exception while committing with ParallelCommitStrategy", e);
            }
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
                            .filter(TransactionRelation::release)
                            .forEach(tasks::add);
                }
            } catch (InterruptedException | IgniteInterruptedException e) {
                LOGGER.error("[R] Exception while committing with ParallelCommitStrategy", e);
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
