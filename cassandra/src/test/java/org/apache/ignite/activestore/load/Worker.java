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

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.log4j.Logger;

/**
 * Worker thread abstraction to be inherited by specific load test implementation
 */
public abstract class Worker extends Thread {
    /** */
    private static final SimpleDateFormat TIME_FORMATTER = new SimpleDateFormat("hh:mm:ss");

    /** */
    private final Ignite ignite;

    /** */
    private final Logger log;

    /** */
    private final LoadTestConfig config;

    /** */
    private final long startPosition;

    /** */
    private final long endPosition;

    /** */
    private final Generator keyGenerator;

    /** */
    private final Generator valueGenerator;

    /** */
    private long testStartTime;

    /** */
    private boolean warmup;

    /** */
    private volatile long warmupStartTime = 0;

    /** */
    private volatile long warmupFinishTime = 0;

    /** */
    private volatile long startTime = 0;

    /** */
    private volatile long finishTime = 0;

    /** */
    private volatile long warmupMsgProcessed = 0;

    /** */
    private volatile long warmupSleepCnt = 0;

    /** */
    private volatile long msgProcessed = 0;

    /** */
    private volatile long msgFailed = 0;

    /** */
    private volatile long sleepCnt = 0;

    /** */
    private Throwable executionError;

    /** */
    private long statReportedTime;

    /** */
    private boolean running = true;

    /** */
    public Worker(Ignite ignite, LoadTestConfig config, long startPosition, long endPosition,
        Generator keyGenerator, Generator valueGenerator) {
        this.ignite = ignite;
        this.config = config;
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.keyGenerator = keyGenerator;
        this.valueGenerator = valueGenerator;
        log = Logger.getLogger(loggerName());
        warmup = config.getWarmupPeriod() != 0;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try {
            execute();
        }
        catch (Throwable e) {
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
     * Returns ratio of unsuccessfully processed entries to the number of all entries processed so far.
     */
    public float getErrorsPercent() {
        if (msgFailed == 0) {
            return 0;
        }
        return msgProcessed + msgFailed == 0 ? 0 : (float)(msgFailed * 100) / (float)(msgProcessed + msgFailed);
    }

    /**
     * Returns number of all entries processed so far.
     */
    public long getMsgProcessed() {
        return msgProcessed;
    }

    /**
     * Name of the logger this worker logs its errors and statistics to.
     */
    protected abstract String loggerName();

    /** */
    protected WorkerEntryProcessor getEntryProcessor(Ignite ignite, LoadTestConfig config) {
        throw new UnsupportedOperationException("Processing is not supported");
    }

    /**
     * Until the warmup and execution of load has not finished iterates over dedicated generator sequence chunk and
     * generates entries to process, after the configured number of entries is generated they are processed
     * (read/written) and performance metrics are logged is there was enough time passed since the last performance
     * metrics report from this worker.
     */
    @SuppressWarnings("unchecked")
    private void execute() {
        testStartTime = System.currentTimeMillis();

        log.info("Test execution started");

        if (warmup) {
            log.info("Warm up period started");
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
                    log.info("Warm up period completed");
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

    /** */
    public void shutdownWorker() {
        running = false;
    }

    /** */
    private void doWork(Map entries) {
        try {
            getEntryProcessor(ignite, config).process(entries);
            updateMetrics(entries.size());
        }
        catch (Throwable e) {
            log.error("Failed to perform operation", e);
            updateErrorMetrics(entries.size());
        }
    }

    /**
     * Returns throughput of this workers specifically achieved at warmup stage.
     */
    public long getWarmUpSpeed() {
        if (warmupMsgProcessed == 0) {
            return 0;
        }
        long finish = warmupFinishTime != 0 ? warmupFinishTime : System.currentTimeMillis();
        long duration = (finish - warmupStartTime - warmupSleepCnt * config.getAdditionalRequestLatency()) / 1000;

        return duration == 0 ? warmupMsgProcessed : warmupMsgProcessed / duration;
    }

    /** */
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
            catch (Throwable ignored) {
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
        if (System.currentTimeMillis() - statReportedTime < config.getReportFrequency()) {
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
            log.info("Warm up messages processed " + warmupMsgProcessed + ", " +
                "speed " + getWarmUpSpeed() + " msg/sec, " + completed + "% completed");
        }
        else {
            log.info("Messages processed " + msgProcessed + ", " +
                "speed " + getSpeed() + " msg/sec, " + completed + "% completed, " +
                "errors " + msgFailed + " / " + String.format("%.2f", getErrorsPercent()).replace(",", ".") + "%");
        }
    }

    /**
     * Logs performance statistics of this worker at end of the load test.
     */
    private void reportTestCompletion() {
        StringBuilder builder = new StringBuilder();

        if (executionError != null) {
            builder.append("Test execution abnormally terminated. ");
        }
        else {
            builder.append("Test execution successfully completed. ");
        }
        builder.append("Statistics: ").append(SystemHelper.LINE_SEPARATOR);
        builder.append("Start time: ").append(TIME_FORMATTER.format(testStartTime)).append(SystemHelper.LINE_SEPARATOR);
        builder.append("Finish time: ").append(TIME_FORMATTER.format(finishTime)).append(SystemHelper.LINE_SEPARATOR);
        builder.append("Duration: ").append((finishTime - testStartTime) / 1000).append(" sec")
            .append(SystemHelper.LINE_SEPARATOR);

        if (config.getWarmupPeriod() > 0) {
            builder.append("Warm up period: ").append(config.getWarmupPeriod() / 1000)
                .append(" sec").append(SystemHelper.LINE_SEPARATOR);
            builder.append("Warm up processed messages: ").append(warmupMsgProcessed).append(SystemHelper.LINE_SEPARATOR);
            builder.append("Warm up processing speed: ").append(getWarmUpSpeed())
                .append(" msg/sec").append(SystemHelper.LINE_SEPARATOR);
        }

        builder.append("Processed messages: ").append(msgProcessed).append(SystemHelper.LINE_SEPARATOR);
        builder.append("Processing speed: ").append(getSpeed()).append(" msg/sec").append(SystemHelper.LINE_SEPARATOR);
        builder.append("Errors: ").append(msgFailed).append(" / ").
            append(String.format("%.2f", getErrorsPercent()).replace(",", ".")).append("%");

        if (executionError != null) {
            log.error(builder.toString(), executionError);
        }
        else {
            log.info(builder.toString());
        }
    }
}
