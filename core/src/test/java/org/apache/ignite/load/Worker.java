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

import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.load.statistics.StatisticsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Worker thread abstraction to be inherited by specific load test implementation
 */
public class Worker implements LifecycleAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
    private static final SimpleDateFormat TIME_FORMATTER = new SimpleDateFormat("hh:mm:ss");

    private final LoadTestConfig config;
    private final StatisticsConfig reportConfig;
    private final WorkerEntryProcessor entryProcessor;
    private final Generator keyGenerator;
    private final Generator valueGenerator;

    private volatile long warmupStartTime = 0;
    private volatile long warmupFinishTime = 0;
    private volatile long startTime = 0;
    private volatile long finishTime = 0;
    private volatile long warmupMsgProcessed = 0;
    private volatile long warmupSleepCnt = 0;
    private volatile long msgProcessed = 0;
    private volatile long msgFailed = 0;
    private volatile long sleepCnt = 0;
    private volatile boolean running = true;

    private long testStartTime;
    private boolean warmup;
    private Throwable executionError;
    private long statReportedTime;

    @Inject
    public Worker(
        LoadTestConfig config,
        StatisticsConfig reportConfig,
        WorkerEntryProcessor entryProcessor,
        @Named(LoadTestDCBConfiguration.KEY_GENERATOR) Generator keyGenerator,
        @Named(LoadTestDCBConfiguration.VALUE_GENERATOR) Generator valueGenerator
    ) {
        this.config = config;
        this.reportConfig = reportConfig;
        this.entryProcessor = entryProcessor;
        this.keyGenerator = keyGenerator;
        this.valueGenerator = valueGenerator;
        warmup = config.getWarmupPeriod() != 0;
    }

    public void run(long startPosition, long endPosition) {
        try {
            execute(startPosition, endPosition);
        }
        catch (Exception e) {
            executionError = e;
            throw new RuntimeException("Test execution abnormally terminated", e);
        }
        finally {
            reportTestCompletion();
        }
    }

    /**
     * Checks if there were exceptions thrown during this worker execution.
     */
    public boolean isFailed() {
        return executionError != null;
    }

    /**
     * Returns current throughput of this worker thread, taking into the account the additional request latency setting
     * so it doesn't affect the returned number.
     */
    public long getSpeed() {
        if (msgProcessed == 0) {
            return 0;
        }
        long finish = finishTime != 0 ? finishTime : System.currentTimeMillis();
        long duration = (finish - startTime - sleepCnt * config.getAdditionalRequestLatency()) / 1000;

        return duration == 0 ? msgProcessed : msgProcessed / duration;
    }

    /**
     * Returns number of unsuccessfully processed entries.
     */
    public long getErrorsCount() {
        return msgFailed;
    }

    /**
     * Returns number of all entries processed so far.
     */
    public long getMsgProcessed() {
        return msgProcessed;
    }

    /**
     * Until the warmup and execution of load has not finished iterates over dedicated generator sequence chunk and
     * generates entries to process, after the configured number of entries is generated they are processed
     * (read/written) and performance metrics are logged is there was enough time passed since the last performance
     * metrics report from this worker.
     */
    @SuppressWarnings("unchecked")
    private void execute(long startPosition, long endPosition) {
        testStartTime = System.currentTimeMillis();

        LOGGER.info("[T] Test execution started");

        if (warmup) {
            LOGGER.info("[T] Warm up period started");
        }
        warmupStartTime = warmup ? testStartTime : 0;
        startTime = !warmup ? testStartTime : 0;

        statReportedTime = testStartTime;

        int batchSize = config.getBatchSize();
        long warmupPeriod = config.getWarmupPeriod();
        long cntr = startPosition;
        Map batchMap = new HashMap(batchSize);

        long execTime = warmupPeriod + config.getWorkerExecutionPeriod();

        try {
            while (running) {
                if (System.currentTimeMillis() - testStartTime > execTime) {
                    break;
                }
                if (warmup && System.currentTimeMillis() - testStartTime > warmupPeriod) {
                    warmupFinishTime = System.currentTimeMillis();
                    startTime = warmupFinishTime;
                    statReportedTime = warmupFinishTime;
                    warmup = false;
                    LOGGER.info("[T] Warm up period completed");
                }

                if (batchMap.size() == batchSize) {
                    doWork(batchMap);
                    batchMap.clear();
                }

                if (cntr == endPosition) {
                    cntr = startPosition;
                }
                else {
                    cntr++;
                }
                Object key = keyGenerator.generate(cntr);
                Object val = valueGenerator.generate(cntr);

                batchMap.put(key, val);

                reportStatistics();
            }
        }
        finally {
            warmupFinishTime = warmupFinishTime != 0 ? warmupFinishTime : System.currentTimeMillis();
            finishTime = System.currentTimeMillis();
        }
    }

    @Override public void start() {
        // Do nothing.
    }

    @Override public void stop() {
        running = false;
    }

    /** */
    private void doWork(Map entries) {
        try {
            entryProcessor.process(entries);
            updateMetrics(entries.size());
        }
        catch (Throwable e) {
            LOGGER.error("[T] Failed to perform operation", e);
            updateErrorMetrics(entries.size());
        }
    }

    /**
     * Returns throughput of this workers specifically achieved at warmup stage.
     */
    private long getWarmUpSpeed() {
        if (warmupMsgProcessed == 0) {
            return 0;
        }
        long finish = warmupFinishTime != 0 ? warmupFinishTime : System.currentTimeMillis();
        long duration = (finish - warmupStartTime - warmupSleepCnt * config.getAdditionalRequestLatency()) / 1000;

        return duration == 0 ? warmupMsgProcessed : warmupMsgProcessed / duration;
    }

    /**
     * Returns ratio of unsuccessfully processed entries to the number of all entries processed so far.
     */
    private String getErrorsPercent() {
        if (msgFailed == 0) {
            return "0";
        }
        return String.format("%.2f", TestsHelper.calculateErrorPercent(msgProcessed, msgFailed)).replace(",", ".");
    }


    private void updateMetrics(int itemsProcessed) {
        if (warmup) {
            warmupMsgProcessed += itemsProcessed;
        }
        else {
            msgProcessed += itemsProcessed;
        }
        if (config.getAdditionalRequestLatency() > 0) {
            try {
                Thread.sleep(config.getAdditionalRequestLatency());

                if (warmup) {
                    warmupSleepCnt++;
                }
                else {
                    sleepCnt++;
                }
            }
            catch (InterruptedException e) {
                // Ignore.
            }
        }
    }

    /** */
    private void updateErrorMetrics(int itemsFailed) {
        if (!warmup) {
            msgFailed += itemsFailed;
        }
    }

    /**
     * Logs performance statistics of this worker.
     */
    private void reportStatistics() {
        if (System.currentTimeMillis() - statReportedTime < reportConfig.getReportFrequency()) {
            return;
        }
        statReportedTime = System.currentTimeMillis();

        long completed = warmup ?
            (statReportedTime - warmupStartTime) * 100 / config.getWarmupPeriod() :
            (statReportedTime - startTime) * 100 / config.getWorkerExecutionPeriod();

        if (completed > 100) {
            completed = 100;
        }
        if (warmup) {
            LOGGER.info("[T] Warm up messages processed {}, speed {} msg/sec, {} % completed", warmupMsgProcessed,
                getWarmUpSpeed(), completed);
        }
        else {
            LOGGER.info("[T] Messages processed {}, speed {} msg/sec, {} % completed, errors {} / {}%",
                msgProcessed, getSpeed(), completed, msgFailed, getErrorsPercent());
        }
    }

    /**
     * Logs performance statistics of this worker at end of the load test.
     */
    private void reportTestCompletion() {
        StringBuilder builder = new StringBuilder();
        String newLine = System.lineSeparator();

        if (executionError != null) {
            builder.append("Test execution abnormally terminated. ");
        }
        else {
            builder.append("Test execution successfully completed. ");
        }
        builder.append("Statistics: ").append(newLine);
        builder.append("Start time: ").append(TIME_FORMATTER.format(testStartTime)).append(newLine);
        builder.append("Finish time: ").append(TIME_FORMATTER.format(finishTime)).append(newLine);
        builder.append("Duration: ").append((finishTime - testStartTime) / 1000).append(" sec")
            .append(newLine);

        if (config.getWarmupPeriod() > 0) {
            builder.append("Warm up period: ").append(config.getWarmupPeriod() / 1000)
                .append(" sec").append(newLine);
            builder.append("Warm up processed messages: ").append(warmupMsgProcessed).append(newLine);
            builder.append("Warm up processing speed: ").append(getWarmUpSpeed())
                .append(" msg/sec").append(newLine);
        }

        builder.append("Processed messages: ").append(msgProcessed).append(newLine);
        builder.append("Processing speed: ").append(getSpeed()).append(" msg/sec").append(newLine);
        builder.append("Errors: ").append(msgFailed).append(" / ").
            append(getErrorsPercent()).append("%");

        if (executionError != null) {
            LOGGER.error("[T] " + builder.toString(), executionError);
        }
        else {
            LOGGER.info("[T] " + builder.toString());
        }
    }
}
