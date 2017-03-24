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

package org.apache.ignite.activestore.impl.subscriber.lead;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.impl.kv.MessageMetadata;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

/**
 * @author Evgeniy_Ignatiev
 * @since 11/29/2016 12:27 PM
 */
public class Lead implements Service {
    public static final String SERVICE_NAME = "subscriberLeadService";

    @IgniteInstanceResource
    private transient Ignite ignite;

    private transient boolean running = true;

    private transient IgniteFuture<?> eventLoopFuture;

    private final transient BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

    private final transient LeadPlanner planner = new LeadPlanner();

    private final transient Map<UUID, List<Long>> availableToCommitTxsBuffer = new ConcurrentHashMap<>();

    @Override public void init(ServiceContext context) {
        // Nothing to do.
    }

    @Override public void cancel(ServiceContext context) {
        running = false;
        eventLoopFuture.cancel(); // Interrupt event loop.
    }

    @Override public void execute(ServiceContext context) {
        eventLoopFuture = ignite.scheduler().runLocal(new InterruptibleEventLoop());
    }

    public List<Long> notifyTransactionsRead(UUID consumerId, List<MessageMetadata> metadatas) {
        if (running) {
            taskQueue.add(new NotifyTask(planner, consumerId, metadatas));
            List<Long> result = availableToCommitTxsBuffer.remove(consumerId);
            return result == null ? Collections.<Long>emptyList() : result;
        }
        return Collections.emptyList();
    }

    public void notifyTransactionCommitted(long transactionsId) {
        if (running) {
            taskQueue.add(new DoneTask(planner, Collections.singletonList(transactionsId)));
        }
    }

    private class InterruptibleEventLoop implements Runnable {
        @Override public void run() {
            try {
                while (running) {
                    List<Runnable> tasks = new ArrayList<>();

                    tasks.add(taskQueue.take());
                    taskQueue.drainTo(tasks);
                    for (Runnable task : tasks) {
                        task.run();
                    }
                    Map<UUID, List<Long>> newlyCommittableTxs = planner.getAvailableToCommitTransactions();

                    for (Map.Entry<UUID, List<Long>> entry : newlyCommittableTxs.entrySet()) {
                        List<Long> availableTxs = availableToCommitTxsBuffer.remove(entry.getKey());

                        planner.markInProgressTransactions(entry.getValue());
                        if (availableTxs != null) {
                            availableTxs.addAll(entry.getValue());
                        } else {
                            availableTxs = entry.getValue();
                        }
                        availableToCommitTxsBuffer.put(entry.getKey(), availableTxs);
                    }
                }
            } catch (InterruptedException e) {
                // Stop service.
            }
        }
    }

    private static class NotifyTask implements Runnable {
        private final LeadPlanner planner;
        private final UUID consumerId;
        private final List<MessageMetadata> txMetadatas;

        NotifyTask(LeadPlanner planner, UUID consumerId, List<MessageMetadata> txMetadatas) {
            this.planner = planner;
            this.consumerId = consumerId;
            this.txMetadatas = txMetadatas;
        }

        @Override public void run() {
            List<TxInfo> txInfos = new ArrayList<>(txMetadatas.size());

            for (MessageMetadata metadata : txMetadatas) {
                txInfos.add(new TxInfo(consumerId, metadata));
            }
            planner.processTxsRead(txInfos);
        }
    }

    private static class DoneTask implements Runnable {
        private final LeadPlanner planner;
        private final List<Long> txIds;

        DoneTask(LeadPlanner planner, List<Long> txIds) {
            this.planner = planner;
            this.txIds = txIds;
        }

        @Override public void run() {
            planner.processTxsCommitted(txIds);
        }
    }
}
