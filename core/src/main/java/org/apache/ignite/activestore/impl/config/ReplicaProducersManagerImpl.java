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

package org.apache.ignite.activestore.impl.config;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.impl.publisher.RemoteKafkaProducer;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 * @author Evgeniy_Ignatiev
 * @since 2/1/2017 7:29 PM
 */
@Singleton
public class ReplicaProducersManagerImpl implements ReplicaProducersManager, ReplicaProducersUpdater, LifecycleAware, Runnable {
    private final Ignite ignite;
    private final ReplicaProducersManagerUpdater producersManagerUpdater;

    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

    private volatile boolean running = false;
    private IgniteFuture<?> taskLoopFuture;

    @Inject
    public ReplicaProducersManagerImpl(Ignite ignite, ReplicaProducersManagerUpdater producersManagerUpdater) {
        this.ignite = ignite;
        this.producersManagerUpdater = producersManagerUpdater;
    }

    @Override public void start() {
        taskLoopFuture = ignite.scheduler().runLocal(this);
    }

    @Override public void stop() {
        running = false;
        taskLoopFuture.cancel();
        producersManagerUpdater.close();
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

    @Override public Collection<RemoteKafkaProducer> getProducers() {
        return producersManagerUpdater.getProducers();
    }

    @Override public void updateConfiguration(Map<UUID, ReplicaConfig> replicasConfigs) {
        taskQueue.add(new UpdateConfigurationTask(producersManagerUpdater, replicasConfigs));
    }

    @Override public void unsubscribe(UUID clusterId, Exception e) {
        taskQueue.add(new UnsubscribeTask(producersManagerUpdater, clusterId, e));
    }

    @Override public void resubscribe(UUID clusterId, ReplicaConfig replicaConfig) {
        taskQueue.add(new ResubscribeTask(producersManagerUpdater, clusterId, replicaConfig));
    }
}
