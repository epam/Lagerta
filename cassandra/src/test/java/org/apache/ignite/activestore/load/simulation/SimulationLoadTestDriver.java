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

package org.apache.ignite.activestore.load.simulation;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.activestore.load.LoadTestConfig;
import org.apache.ignite.activestore.load.LoadTestDriver;
import org.apache.ignite.activestore.load.generators.LongGenerator;
import org.apache.log4j.Logger;

/**
 * Load test driver created specifically for simulation load tests to incorporate boilerplate configuration routines.
 */
public class SimulationLoadTestDriver extends LoadTestDriver {
    /** Name of ignite atomic holding total number of workers started across the cluster. */
    private static final String GLOBAL_WORKER_NUMBER = "globalWorkerThreadNumber";

    /** */
    private static final String SIMULATION_TEST_NAME = "Simulation";

    /** */
    private final Ignite ignite;

    /** */
    public SimulationLoadTestDriver(Logger logger, LoadTestConfig config, Ignite ignite) {
        super(logger, config, new LongGenerator(), new TransactionDataGenerator());
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override protected Ignite setup() {
        return ignite;
    }

    /** {@inheritDoc} */
    @Override protected void tearDown(Ignite ignite) {
        // Do nothing.
    }

    /** */
    private static IgniteAtomicLong getGlobalWorkerNumber(Ignite ignite) {
        return ignite.atomicLong(GLOBAL_WORKER_NUMBER, 0, true);
    }

    /** {@inheritDoc} */
    @Override protected long generateStartPosition(Ignite ignite, int workerNumber, LoadTestConfig config) {
        long offset = SimulationUtil.WORKER_OFFSET * getGlobalWorkerNumber(ignite).getAndIncrement();
        return config.getTestInitialPosition() + offset;
    }

    /** {@inheritDoc} */
    @Override protected long generateEndPosition(LoadTestConfig config, long startPosition) {
        return startPosition + SimulationUtil.WORKER_OFFSET;
    }

    /** */
    public static void runSimulation(Logger logger, Ignite ignite, int workers, long warmupPeriod, long executionPeriod,
        long additionalRequestLatency, int batchSize, long reportFrequency) {
        LoadTestConfig config = LoadTestConfig.defaultConfig()
            .setThreads(workers)
            .setWorkerExecutionPeriod(executionPeriod)
            .setWarmupPeriod(warmupPeriod)
            .setAdditionalRequestLatency(additionalRequestLatency)
            .setBatchSize(batchSize)
            .setReportFrequency(reportFrequency);
        runSimulation(logger, config, ignite);
    }

    /** */
    public static void runSimulation(Logger logger, LoadTestConfig config, Ignite ignite) {
        config = config
            .setTestName(SIMULATION_TEST_NAME)
            .setClazz(SimulationWorker.class);
        new SimulationLoadTestDriver(logger, config, ignite).runTest();
    }

    /** */
    public static void runSimulation(Logger logger, Ignite ignite) {
        runSimulation(logger, LoadTestConfig.defaultConfig(), ignite);
    }
}
