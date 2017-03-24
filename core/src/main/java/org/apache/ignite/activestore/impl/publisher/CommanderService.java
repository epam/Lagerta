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

package org.apache.ignite.activestore.impl.publisher;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;
import org.apache.ignite.lang.IgniteFuture;

/**
 * @author Evgeniy_Ignatiev
 * @since 2/1/2017 5:40 PM
 */
@Singleton
public class CommanderService implements Runnable {
    private final Ignite ignite;
    private final Commander commander;
    private final Reconciler reconciler;

    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

    private IgniteFuture<?> taskLoopFuture;

    private volatile boolean running = false;

    @Inject
    public CommanderService(Ignite ignite, Commander commander, Reconciler reconciler) {
        this.ignite = ignite;
        this.commander = commander;
        this.reconciler = reconciler;
    }

    public void register(UUID replicaId, ReplicaConfig replicaConfig, boolean isMain) {
        taskQueue.add(new RegisterReplicaTask(commander, replicaId, replicaConfig, isMain));
    }

    public void registerAll(UUID[] ids, ReplicaConfig[] configs, UUID mainClusterId) {
        taskQueue.add(new AllReplicasRegisterTask(commander, ids, configs, mainClusterId));
    }

    public void startReconciliation(UUID replicaId, long startTransactionId) {
        taskQueue.add(new StartReconciliationTask(reconciler, replicaId, startTransactionId));
    }

    public void stopReconciliation(UUID replicaId) {
        taskQueue.add(new StopReconciliationTask(reconciler, replicaId));
    }

    public void notifyNodeUnsubscribedFromReplica(UUID replicaId) {
        taskQueue.add(new UnsubscriptionNotificationTask(commander, replicaId));
    }

    public void execute() {
        commander.init();
        taskLoopFuture = ignite.scheduler().runLocal(this);
    }

    public void cancel() {
        running = false;
        taskLoopFuture.cancel();
        commander.close();
    }

    @Override public void run() {
        running = true;
        try {
            while (running) {
                taskQueue.take().run();
            }
        } catch (InterruptedException e) {
            // Stop execution.
        }
    }
}
