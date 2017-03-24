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

package org.apache.ignite.load.simulation;

import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.load.LoadTestConfig;
import org.apache.ignite.load.LoadTestDriver;
import org.apache.ignite.load.Worker;
import org.apache.ignite.load.statistics.Statistics;

/**
 * Load test driver created specifically for simulation load tests to incorporate boilerplate configuration routines.
 */
public class SimulationLoadTestDriver extends LoadTestDriver {
    /** Name of ignite atomic holding total number of workers started across the cluster. */
    private static final String GLOBAL_WORKER_NUMBER = "globalWorkerThreadNumber";

    @Inject
    public SimulationLoadTestDriver(
        Ignite ignite,
        Statistics stats,
        LoadTestConfig config,
        Provider<? extends Worker> workersProvider
    ) {
        super(ignite, stats, config, workersProvider);
    }

    private static IgniteAtomicLong getGlobalWorkerNumber(Ignite ignite) {
        return ignite.atomicLong(GLOBAL_WORKER_NUMBER, 0, true);
    }

    /** {@inheritDoc} */
    @Override protected long generateStartPosition(int workerNumber) {
        long offset = SimulationUtil.WORKER_OFFSET * getGlobalWorkerNumber(ignite).getAndIncrement();
        return config.getTestInitialPosition() + offset;
    }

    /** {@inheritDoc} */
    @Override protected long generateEndPosition(long startPosition) {
        return startPosition + SimulationUtil.WORKER_OFFSET;
    }
}
