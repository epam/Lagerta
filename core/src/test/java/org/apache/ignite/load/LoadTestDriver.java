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

import org.apache.ignite.Ignite;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.load.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Basic load test driver to be inherited by specific implementation for particular use-case.
 */
public abstract class LoadTestDriver implements LifecycleAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadTestDriver.class);
    private static final int WORKER_JOIN_TIMEOUT = 30000;

    protected final Ignite ignite;
    protected final Statistics stats;
    protected final LoadTestConfig config;
    protected final Provider<? extends Worker> workersProvider;

    private final Queue<WorkerThread> workers = new ConcurrentLinkedQueue<>();
    private final List<String> failedWorkerNames = new ArrayList<>();

    private volatile boolean running = true;

    private boolean workersStopped = false;

    public LoadTestDriver(
        Ignite ignite,
        Statistics stats,
        LoadTestConfig config,
        Provider<? extends Worker> workersProvider
    ) {
        this.ignite = ignite;
        this.stats = stats;
        this.config = config;
        this.workersProvider = workersProvider;
    }

    @Override public void start() {
        // Do nothing.
    }

    @Override public void stop() {
        running = false;
    }

    /**
     * Runs load test with user defined config.
     */
    public void run() throws Exception {
        LOGGER.info("[T] Setting up load tests driver");
        setupDriver();
        LOGGER.info("[T] Load tests driver setup successfully completed");
        LOGGER.info("[T] Running {} test", config.getTestName());
        try {
            LOGGER.info("[T] Waiting for test to finish.");
            while (running) {
                awaitWorkersShutdown();
            }
            printTestResultsHeader();
            printTestResultsStatistics();
        } finally {
            running = false;
            stopDriver();
        }
    }

    public void startAdditionalWorkerThreads() {
        if (!running) {
            return;
        }
        LOGGER.info("[T] Starting workers");

        for (int i = 0; i < config.getThreads(); i++) {
            int currentWorkers = workers.size();
            long startPosition = generateStartPosition(currentWorkers);
            long endPosition = generateEndPosition(startPosition);
            WorkerThread workerThread = new WorkerThread(
                workersProvider.get(),
                "Worker-" + currentWorkers,
                startPosition,
                endPosition
            );
            workerThread.start();
            workers.add(workerThread);
        }
        stats.recordWorkersThreadStarted(config.getThreads());
        LOGGER.info("[T] Workers started");
    }

    private void awaitWorkersShutdown() {
        for (WorkerThread worker : workers) {
            try {
                awaitWorkerShutdown(worker);
            }
            catch (InterruptedException e) {
                LOGGER.error("[T] Worker " + worker.getName() + " waiting interrupted", e);
            }
            if (worker.isFailed()) {
                failedWorkerNames.add(worker.getName());
                LOGGER.info("[T] Worker {} execution failed", worker.getName());
            }
            else {
                LOGGER.info("[T] Worker {} successfully completed", worker.getName());
            }
        }
    }

    private void awaitWorkerShutdown(WorkerThread worker) throws InterruptedException {
        while (worker.isAlive()) {
            if (!running && !workersStopped) {
                // Stop all the workers eagerly to speed up the shutdown process.
                for (WorkerThread workerThread : workers) {
                    workerThread.stop();
                }
                workersStopped = true;
            }
            worker.join(WORKER_JOIN_TIMEOUT);
        }
    }

    /**
     * Generates start position in generator sequence for a specific worker thread.
     *
     * @param workerNumber Number of the worker thread.
     * @return Start position of generator sequence chunk for the worker thread.
     */
    protected abstract long generateStartPosition(int workerNumber);

    /**
     * Generates end position of the generator sequence chunk based on the start position for that chunk.
     *
     * @param startPosition Start position of the generator sequence chunk.
     * @return End position of the generator sequence chunk.
     */
    protected long generateEndPosition(long startPosition) {
        return startPosition + config.getPositionStep();
    }

    @SuppressWarnings("unchecked")
    protected void setupDriver() throws Exception {
        ignite.atomicLong(LoadTestDriversCoordinator.STARTED_DRIVERS, 0, false).incrementAndGet();
    }

    @SuppressWarnings("unchecked")
    private void stopDriver() throws Exception {
        awaitWorkersShutdown();
        ignite.atomicLong(LoadTestDriversCoordinator.STARTED_DRIVERS, 0, false).close();
    }

    private void printTestResultsHeader() {
        String testName = config.getTestName();

        if (failedWorkerNames.isEmpty()) {
            LOGGER.info("[T] {} test execution successfully completed.", testName);
            return;
        }
        if (failedWorkerNames.size() == workers.size()) {
            LOGGER.error("[T] {} test execution totally failed.", testName);
            return;
        }
        String strFailedWorkers = "";

        for (String workerName : failedWorkerNames) {
            if (!strFailedWorkers.isEmpty()) {
                strFailedWorkers += ", ";
            }
            strFailedWorkers += workerName;
        }
        LOGGER.warn("[T] {} test execution completed, but {} of {} workers failed. Failed workers: {}",
            testName, failedWorkerNames.size(), workers.size(), strFailedWorkers);
    }

    /**
     * Logs performance statistics aggregated for all the worker threads this driver executed.
     */
    @SuppressWarnings("StringBufferReplaceableByString")
    private void printTestResultsStatistics() {
        long totalMsgProcessed = 0;
        long totalMsgFailed = 0;
        long totalSpeed = 0;

        for (WorkerThread worker : workers) {
            totalMsgProcessed += worker.getMsgProcessed();
            totalMsgFailed += worker.getErrorsCount();
            totalSpeed += worker.getSpeed();
        }
        double totalErrorPercent = TestsHelper.calculateErrorPercent(totalMsgProcessed, totalMsgFailed);
        StringBuilder builder = new StringBuilder();
        String testName = config.getTestName();

        builder.append(System.lineSeparator());
        builder.append("-------------------------------------------------");
        builder.append(System.lineSeparator());
        builder.append(testName).append(" test statistics").append(System.lineSeparator());
        builder.append(testName).append(" messages: ").append(totalMsgProcessed).append(System.lineSeparator());
        builder.append(testName).append(" errors: ").append(totalMsgFailed).append(", ").
            append(String.format("%.2f", totalErrorPercent).replace(",", ".")).
            append("%").append(System.lineSeparator());
        builder.append(testName).append(" speed: ").append(totalSpeed).append(" msg/sec").append(System.lineSeparator());
        builder.append("-------------------------------------------------");

        LOGGER.info("[T] " + builder.toString());
    }
}
