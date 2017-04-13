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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.impl.transactions.JointTxScope;
import org.apache.ignite.activestore.subscriber.Committer;
import org.apache.ignite.activestore.subscriber.TransactionSupplier;
import org.apache.ignite.activestore.transactions.TransactionScopeIterator;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Andrei_Yakushin
 * @since 1/9/2017 9:43 AM
 */
public class ParallelIgniteCommitter implements Committer, LifecycleAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelIgniteCommitter.class);

    private static final int POOL_COUNT = 5;
    public static final int TERMINATION_TIMEOUT = 5;

    private final Ignite ignite;

    private ExecutorService executor;

    @Inject
    public ParallelIgniteCommitter(Ignite ignite) {
        this.ignite = ignite;
    }

    @Override
    public void commitAsync(
        LongList txIds,
        TransactionSupplier txSupplier,
        IgniteInClosure<Long> onSingleCommit, IgniteInClosure<LongList> onFullCommit
    ) {
        new CommitterContext(ignite, executor, txSupplier, onSingleCommit, onFullCommit, txIds).schedule();
    }

    @Override
    public void start() throws IgniteException {
        executor = Executors.newFixedThreadPool(POOL_COUNT);
    }

    @Override
    public void stop() throws IgniteException {
        try {
            LOGGER.info("[C] attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOGGER.warn("[C] tasks interrupted");
        }
        finally {
            if (!executor.isTerminated()) {
                LOGGER.warn("[C] cancel non-finished tasks");
            }
            executor.shutdownNow();
            LOGGER.info("[C] shutdown finished");
        }
    }

    private static class CommitterContext {
        private final Ignite ignite;
        private final ExecutorService executor;
        private final TransactionSupplier txSupplier;
        private final IgniteInClosure<Long> onSingleCommit;
        private final IgniteInClosure<LongList> onFullCommit;

        private final LongList ready;
        private final MutableLongSet wip;
        private final MutableLongSet committed;

        public CommitterContext(
                Ignite ignite,
                ExecutorService executor,
                TransactionSupplier txSupplier,
                IgniteInClosure<Long> onSingleCommit,
                IgniteInClosure<LongList> onFullCommit,
                LongList ready
        ) {
            this.ignite = ignite;
            this.ready = ready;
            this.txSupplier = txSupplier;
            this.onSingleCommit = onSingleCommit;
            this.onFullCommit = onFullCommit;
            this.executor = executor;

            wip = new LongHashSet();
            committed = new LongHashSet();
        }

        public void schedule() {
            for (LongIterator it = getAvailableTasks().longIterator(); it.hasNext(); ) {
                executor.submit(new Task(ignite, txSupplier, onSingleCommit, this, it.next()));
            }
        }

        public void notifyCommitted(long committedId) {
            synchronized (this) {                  //todo it could be optimized by extracting planning to separate class
                committed.add(committedId);
            }
            schedule();
        }

        private LongList getAvailableTasks() {
            MutableLongList tasks = new LongArrayList();
            boolean allDone = true;
            synchronized (this) {
                JointTxScope blocked = new JointTxScope();
                for (LongIterator it = ready.longIterator(); it.hasNext(); ) {
                    long id = it.next();
                    if (!committed.contains(id)) {
                        TransactionScopeIterator scopeIt = txSupplier.scopeIterator(id);
                        boolean isBlocked = blocked.addAll(scopeIt);

                        allDone = false;
                        if (!wip.contains(id) && isBlocked) {
                            tasks.add(id);
                        }
                    }
                }
                wip.addAll(tasks);
            }
            if (allDone) {
                onFullCommit.apply(ready);
            }
            return tasks;
        }

    }

    private static class Task extends AbstractIgniteCommitter implements Runnable {
        private final TransactionSupplier txSupplier;
        private final IgniteInClosure<Long> onSingleCommit;
        private final CommitterContext committerContext;

        private final long id;

        public Task(
            Ignite ignite,
            TransactionSupplier txSupplier,
            IgniteInClosure<Long> onSingleCommit,
            CommitterContext committerContext,
            long id
        ) {
            super(ignite);
            this.txSupplier = txSupplier;
            this.onSingleCommit = onSingleCommit;
            this.committerContext = committerContext;
            this.id = id;
        }

        @Override
        public void run() {
            Thread currentThread = Thread.currentThread();
            String oldName = currentThread.getName();
            currentThread.setName("Commit transaction-" + id);
            try {
                singleCommit(txSupplier, onSingleCommit, id);
                committerContext.notifyCommitted(id);
            } finally {
                currentThread.setName(oldName);
            }
        }
    }
}
