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

package org.apache.ignite.load;

import java.util.Timer;

import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteRunnable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;

public class LoadTestDriversCoordinator implements LifecycleAware, Runnable {
    private static final long DRIVERS_STARTED_POLL_TIMEOUT = 10000;

    static final String STARTED_DRIVERS = "startedDriversCounter";

    private final Ignite ignite;
    private final LoadTestConfig config;

    private IgniteFuture future;

    private volatile boolean running = true;

    @Inject
    public LoadTestDriversCoordinator(Ignite ignite, LoadTestConfig config, Timer timer) {
        this.ignite = ignite;
        this.config = config;
    }

    @Override public void start() {
        // Do nothing.
    }

    @Override public void stop() {
        running = false;
        future.cancel();
    }

    public void startCoordinator() {
        future = ignite.scheduler().runLocal(this);
    }

    public void awaitCoordinatorStop() {
        future.get();
    }

    @Override public void run() {
        IgniteAtomicLong startedDrivers = ignite.atomicLong(STARTED_DRIVERS, 0, true);

        try {
            // Wait till all the drivers have been started.
            while (running && startedDrivers.get() < config.getTestClientsNumber()) {
                Thread.sleep(DRIVERS_STARTED_POLL_TIMEOUT);
            }
            if (!config.isGradualLoadIncreaseEnabled()) {
                startWorkersRemotely();
            }
            long startTime = System.currentTimeMillis();
            long fullExecutionTime = config.getWarmupPeriod() + config.getWorkerExecutionPeriod();

            while (running && System.currentTimeMillis() - startTime < fullExecutionTime) {
                startWorkersRemotely();
                Thread.sleep(config.getGradualLoadIncreasePeriodicity());
            }
        } catch (InterruptedException e) {
            // Do nothing.
        }
    }

    private void startWorkersRemotely() {
        ignite.compute(ignite.cluster().forClients()).broadcast(new WorkersStarter());
    }

    private static class WorkersStarter extends ActiveStoreIgniteRunnable {
        @IgniteInstanceResource
        private transient Ignite ignite;

        @Inject
        private transient LoadTestDriver driver;

        @Override public void runInjected() {
            driver.startAdditionalWorkerThreads();
        }
    }
}
