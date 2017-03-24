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

package org.apache.ignite.activestore.load;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/** */
public class LoadTestDriversCoordinator {
    /** */
    private static final long DRIVERS_STARTED_POLL_TIMEOUT = 10000;

    /** */
    static final String STARTED_DRIVERS = "startedDriversCounter";

    /** */
    private final Ignite ignite;

    /** */
    private final LoadTestConfig config;

    /** */
    public LoadTestDriversCoordinator(Ignite ignite, LoadTestConfig config) {
        this.ignite = ignite;
        this.config = config;
    }

    /** */
    public void coordinateWorkersStart() {
        IgniteAtomicLong startedDrivers = ignite.atomicLong(STARTED_DRIVERS, 0, true);

        // Wait till all the drivers have been started.
        while (startedDrivers.get() < config.getTestClientsNumber()) {
            try {
                Thread.sleep(DRIVERS_STARTED_POLL_TIMEOUT);
            } catch (InterruptedException e) {
                // Do nothing.
            }
        }
        if (config.isGradualLoadIncreaseEnabled()) {
            long startTime = System.currentTimeMillis();
            long fullExecutionTime = config.getWarmupPeriod() + config.getWorkerExecutionPeriod();

            while (System.currentTimeMillis() - startTime < fullExecutionTime) {
                try {
                    startWorkersRemotely();
                    Thread.sleep(config.getGradualLoadIncreasePeriodicity());
                }
                catch (InterruptedException e) {
                    // Do nothing.
                }
            }
        } else {
            startWorkersRemotely();
        }
    }

    /** */
    public static void runCoordinator(Ignite ignite, LoadTestConfig config) {
        final LoadTestDriversCoordinator coordinator = new LoadTestDriversCoordinator(ignite, config);
        Thread coordinatorThread = new Thread(new Runnable() {
            @Override public void run() {
                coordinator.coordinateWorkersStart();
            }
        });
        coordinatorThread.setDaemon(true);
        coordinatorThread.start();
    }

    /** */
    private void startWorkersRemotely() {
        ignite.compute(ignite.cluster().forClients()).broadcast(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                LoadTestDriver.startAdditionalWorkerThreads(ignite);
            }
        });
    }
}
