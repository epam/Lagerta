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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.load.statistics.Statistics;
import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.log4j.Logger;

/**
 * Basic load test driver to be inherited by specific implementation for particular use-case.
 */
public class LoadTestDriver {
    /** */
    public static String STOPPED = "loadTestDriverStopped";

    /** */
    private static final String DRIVER_ENTRY = "localLoadTestDriver";

    /** Initial position of generator sequence if none is configured. */
    private static final int DEFAULT_INITIAL_POSITION = 0;

    /** Default size of chunk of generator sequence per each worker thread. */
    private static final int DEFAULT_POSITION_STEP = 100000000;

    /** Number of attempts to setup load test */
    private static final int NUMBER_OF_SETUP_ATTEMPTS = 10;

    /** Timeout between load test setup attempts */
    private static final int SETUP_ATTEMPT_TIMEOUT = 1000;

    /** */
    private static final int WORKER_JOIN_TIMEOUT = 30000;

    /** */
    private final Logger logger;

    /** Generator of entry keys. */
    private final Generator keyGenerator;

    /** Generator of entry values. */
    private final Generator valueGenerator;

    /** */
    private final LoadTestConfig config;

    /** */
    private final List<Worker> workers = new ArrayList<>();

    /** Constructs new driver with the generators configured from properties file. */
    public LoadTestDriver(Logger logger, LoadTestConfig config) {
        this(logger, config, TestsHelper.getLoadTestsKeyGenerator(), TestsHelper.getLoadTestsValueGenerator());
    }

    /** */
    public LoadTestDriver(Logger logger, LoadTestConfig config, Generator keyGenerator, Generator valueGenerator) {
        this.logger = logger;
        this.config = config;
        this.keyGenerator = keyGenerator;
        this.valueGenerator = valueGenerator;
    }

    /**
     * Runs load test for the given worker with the default configuration.
     *
     * @param testName Name of the test to be used for logging purposes.
     * @param clazz Class of the worker to be used to instantiate worker threads.
     * @param transactional Determines if the worker should perform its work in transactions.
     */
    public static void runTest(Logger logger, String testName, Class<? extends Worker> clazz, boolean transactional) {
        LoadTestConfig config = LoadTestConfig.defaultConfig()
            .setTestName(testName)
            .setClazz(clazz)
            .setPositionStep(DEFAULT_POSITION_STEP)
            .setTestInitialPosition(DEFAULT_INITIAL_POSITION)
            .setTransactional(transactional);
        new LoadTestDriver(logger, config).runTest();
    }

    /** */
    private boolean shouldStop(Ignite ignite) {
        Map nodeLocalMap = ignite.cluster().nodeLocalMap();

        if (nodeLocalMap.containsKey(STOPPED)) {
            return (Boolean) nodeLocalMap.get(STOPPED);
        }
        return false;
    }

    /**
     * Runs load test with user defined config.
     */
    public void runTest() {
        String testName = config.getTestName();

        logger.info("Running " + testName + " test");

        Ignite ignite = null;

        int attempt;

        logger.info("Setting up load tests driver");

        for (attempt = 0; attempt < NUMBER_OF_SETUP_ATTEMPTS; attempt++) {
            try {
                ignite = setupDriver();
                break;
            }
            catch (Throwable e) {
                logger.error((attempt + 1) + " attempt to setup load test '" + testName + "' failed", e);
            }

            if (attempt + 1 != NUMBER_OF_SETUP_ATTEMPTS) {
                logger.info("Sleeping for " + SETUP_ATTEMPT_TIMEOUT + " seconds before trying next attempt " +
                    "to setup '" + testName + "' load test");
                try {
                    Thread.sleep(SETUP_ATTEMPT_TIMEOUT);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }
            }
        }

        if (ignite == null && attempt == NUMBER_OF_SETUP_ATTEMPTS) {
            throw new RuntimeException("All " + NUMBER_OF_SETUP_ATTEMPTS + " attempts to setup load test '" +
                testName + "' have failed");
        }

        logger.info("Load tests driver setup successfully completed");

        try {
            logger.info("Waiting for test to finish.");

            List<String> failedWorkers = new LinkedList<>();

            while (!shouldStop(ignite)) {
                for (Worker worker : workers) {
                    try {
                        while (worker.isAlive()) {
                            if (shouldStop(ignite)) {
                                worker.shutdownWorker();
                            }
                            worker.join(WORKER_JOIN_TIMEOUT);
                        }
                    }
                    catch (InterruptedException e) {
                        logger.error("Worker " + worker.getName() + " waiting interrupted", e);
                    }
                    if (worker.isFailed()) {
                        failedWorkers.add(worker.getName());
                        logger.info("Worker " + worker.getName() + " execution failed");
                    }
                    else {
                        logger.info("Worker " + worker.getName() + " successfully completed");
                    }
                }
                try {
                    Thread.sleep(WORKER_JOIN_TIMEOUT);
                } catch (InterruptedException e) {
                    // Do nothing.
                }
            }
            printTestResultsHeader(testName, failedWorkers, config.getThreads());
            printTestResultsStatistics(testName, workers);
        } finally {
            stopDriver(ignite);
        }
    }

    /** */
    private void startWorkers(Ignite ignite) {
        logger.info("Starting workers");

        for (int i = 0; i < config.getThreads(); i++) {
            int currentWorkers = workers.size();
            long startPosition = generateStartPosition(ignite, currentWorkers, config);
            long endPosition = generateEndPosition(config, startPosition);
            Worker worker = createWorker(ignite, config, startPosition, endPosition);

            worker.setName(config.getTestName() + "-worker-" + currentWorkers);
            worker.start();
            workers.add(worker);
        }
        Statistics.recordWorkersThreadStarted(config.getThreads());
        logger.info("Workers started");
    }

    /**
     * Generates start position in generator sequence for a specific worker thread.
     *
     * @param ignite Ignite client instance.
     * @param workerNumber Number of the worker thread.
     * @param config Load test configuration.
     * @return Start position of generator sequence chunk for the worker thread.
     */
    protected long generateStartPosition(Ignite ignite, int workerNumber, LoadTestConfig config) {
        return getHostUniquePrefix()
            + config.getTestInitialPosition()
            + workerNumber * (config.getPositionStep() + 1);
    }

    /**
     * Generates end position of the generator sequence chunk based on the start position for that chunk.
     *
     * @param config Load test configuration.
     * @param startPosition Start position of the generator sequence chunk.
     * @return End position of the generator sequence chunk.
     */
    protected long generateEndPosition(LoadTestConfig config, long startPosition) {
        return startPosition + config.getPositionStep();
    }

    /**
     * Setups connection to the ignite cluster from this driver.
     */
    protected Ignite setup() {
        return TestsHelper.getClusterClient();
    }

    /** */
    @SuppressWarnings("unchecked")
    private Ignite setupDriver() {
        Ignite ignite = setup();

        ignite.cluster().nodeLocalMap().put(DRIVER_ENTRY, this);
        ignite.atomicLong(LoadTestDriversCoordinator.STARTED_DRIVERS, 0, false).incrementAndGet();
        return ignite;
    }

    /**
     * Stops this driver by performing necessary cleanup actions.
     */
    protected void tearDown(Ignite ignite) {
        if (ignite != null) {
            ignite.close();
        }
    }

    /** */
    @SuppressWarnings("unchecked")
    private void stopDriver(Ignite ignite) {
        ignite.cluster().nodeLocalMap().remove(DRIVER_ENTRY);
        tearDown(ignite);

    }

    /**
     * Instantiates worker thread based on the given configuration.
     *
     * @param ignite Ignite client to pass to the worker.
     * @param config Load test configuration.
     * @param startPosition Start position of chunk of generator sequence this worker will use to generate its load
     * data.
     * @param endPosition End position of chunk of generator sequence.
     */
    private Worker createWorker(Ignite ignite, LoadTestConfig config, long startPosition, long endPosition) {
        Class<? extends Worker> clazz = config.getClazz();

        try {
            Constructor ctor = clazz.getConstructor(Ignite.class, LoadTestConfig.class, long.class, long.class,
                Generator.class, Generator.class);
            return (Worker)ctor.newInstance(ignite, config, startPosition, endPosition, keyGenerator, valueGenerator);
        }
        catch (Throwable e) {
            logger.error("Failed to instantiate worker of class '" + clazz.getName() + "'", e);
            throw new RuntimeException("Failed to instantiate worker of class '" + clazz.getName() + "'", e);
        }
    }

    /** */
    private void printTestResultsHeader(String testName, List<String> failedWorkers, int workersNumbers) {
        if (failedWorkers.isEmpty()) {
            logger.info(testName + " test execution successfully completed.");
            return;
        }

        if (failedWorkers.size() == workersNumbers) {
            logger.error(testName + " test execution totally failed.");
            return;
        }

        String strFailedWorkers = "";

        for (String workerName : failedWorkers) {
            if (!strFailedWorkers.isEmpty()) {
                strFailedWorkers += ", ";
            }
            strFailedWorkers += workerName;
        }

        logger.warn(testName + " test execution completed, but " + failedWorkers.size() + " of " +
            workersNumbers + " workers failed. Failed workers: " + strFailedWorkers);
    }

    /**
     * Logs performance statistics aggregated for all the worker threads this driver executed.
     */
    @SuppressWarnings("StringBufferReplaceableByString")
    private void printTestResultsStatistics(String testName, List<Worker> workers) {
        long cnt = 0;
        long errCnt = 0;
        long speed = 0;

        for (Worker worker : workers) {
            cnt += worker.getMsgProcessed();
            errCnt += worker.getErrorsCount();
            speed += worker.getSpeed();
        }

        float errPercent = errCnt == 0 ?
            0 :
            cnt + errCnt == 0 ? 0 : (float)(errCnt * 100) / (float)(cnt + errCnt);

        StringBuilder builder = new StringBuilder();
        builder.append(SystemHelper.LINE_SEPARATOR);
        builder.append("-------------------------------------------------");
        builder.append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" test statistics").append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" messages: ").append(cnt).append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" errors: ").append(errCnt).append(", ").
            append(String.format("%.2f", errPercent).replace(",", ".")).
            append("%").append(SystemHelper.LINE_SEPARATOR);
        builder.append(testName).append(" speed: ").append(speed).append(" msg/sec").append(SystemHelper.LINE_SEPARATOR);
        builder.append("-------------------------------------------------");

        logger.info(builder.toString());
    }

    // Calculates host unique prefix based on its subnet IP address.
    private long getHostUniquePrefix() {
        String[] parts = SystemHelper.HOST_IP.split("\\.");

        if (parts[2].equals("0")) {
            parts[2] = "777";
        }
        if (parts[3].equals("0")) {
            parts[3] = "777";
        }
        long part3 = Long.parseLong(parts[2]);
        long part4 = Long.parseLong(parts[3]);

        if (part3 < 10) {
            part3 = part3 * 100;
        }
        else if (part4 < 100) {
            part3 = part3 * 10;
        }
        if (part4 < 10) {
            part4 = part4 * 100;
        }
        else if (part4 < 100) {
            part4 = part4 * 10;
        }
        return (part4 * 100000000000000L) + (part3 * 100000000000L) + Thread.currentThread().getId();
    }

    /** */
    public static void startAdditionalWorkerThreads(Ignite ignite) {
        LoadTestDriver driver = (LoadTestDriver)ignite.cluster().nodeLocalMap().get(DRIVER_ENTRY);

        if (driver != null) {
            driver.startWorkers(ignite);
        }
    }
}
