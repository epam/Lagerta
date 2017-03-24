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

/**
 * Allows to programmatically configure options defined in load test properties in a fluent way.
 */
public class LoadTestConfig {
    /** */
    private int batchSize;

    /** */
    private long additionalRequestLatency;

    /** */
    private boolean transactional;

    /** */
    private String cacheName;

    /** */
    private long warmupPeriod;

    /** */
    private long workerExecutionPeriod;

    /** */
    private long reportFrequency;

    /** */
    private int positionStep;

    /** */
    private Class<? extends Worker> clazz;

    /** */
    private String testName;

    /** */
    private int threads;

    /** */
    private long testInitialPosition;

    /** */
    private boolean gradualLoadIncreaseEnabled;

    /** */
    private long gradualLoadIncreasePeriodicity;

    /** */
    private int testClientsNumber;

    /**
     * Creates config filled with values extracted from load test configuration properties.
     */
    public static LoadTestConfig defaultConfig() {
        return new LoadTestConfig()
            .setAdditionalRequestLatency(TestsHelper.getLoadTestsRequestsLatency())
            .setBatchSize(TestsHelper.getBulkOperationSize())
            .setCacheName(TestsHelper.getLoadTestsCacheName())
            .setWarmupPeriod(TestsHelper.getLoadTestsWarmupPeriod())
            .setReportFrequency(TestsHelper.getLoadTestsStatisticsReportFrequency())
            .setWorkerExecutionPeriod(TestsHelper.getLoadTestsExecutionTime())
            .setThreads(TestsHelper.getLoadTestsThreadsCount())
            .setGradualLoadIncreaseEnabled(TestsHelper.isLoadTestsGradualLoadIncreaseEnabled())
            .setGradualLoadIncreasePeriodicity(TestsHelper.getLoadTestsGradualLoadIncreasePeriodicity())
            .setTestClientsNumber(TestsHelper.getLoadTestsClientsNumber());
    }

    /** */
    public int getThreads() {
        return threads;
    }

    /** */
    public LoadTestConfig setThreads(int threads) {
        this.threads = threads;
        return this;
    }

    /** */
    public int getBatchSize() {
        return batchSize;
    }

    /** */
    public LoadTestConfig setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /** */
    public long getAdditionalRequestLatency() {
        return additionalRequestLatency;
    }

    /** */
    public LoadTestConfig setAdditionalRequestLatency(long additionalRequestLatency) {
        this.additionalRequestLatency = additionalRequestLatency;
        return this;
    }

    /** */
    public boolean isTransactional() {
        return transactional;
    }

    /** */
    public LoadTestConfig setTransactional(boolean transactional) {
        this.transactional = transactional;
        return this;
    }

    /** */
    public String getCacheName() {
        return cacheName;
    }

    /** */
    public LoadTestConfig setCacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    /** */
    public long getWarmupPeriod() {
        return warmupPeriod;
    }

    /** */
    public LoadTestConfig setWarmupPeriod(long warmupPeriod) {
        this.warmupPeriod = warmupPeriod;
        return this;
    }

    /** */
    public long getWorkerExecutionPeriod() {
        return workerExecutionPeriod;
    }

    /** */
    public LoadTestConfig setWorkerExecutionPeriod(long workerExecutionPeriod) {
        this.workerExecutionPeriod = workerExecutionPeriod;
        return this;
    }

    /** */
    public long getReportFrequency() {
        return reportFrequency;
    }

    /** */
    public LoadTestConfig setReportFrequency(long reportFrequency) {
        this.reportFrequency = reportFrequency;
        return this;
    }

    /** */
    public int getPositionStep() {
        return positionStep;
    }

    /** */
    public LoadTestConfig setPositionStep(int positionStep) {
        this.positionStep = positionStep;
        return this;
    }

    /** */
    public Class<? extends Worker> getClazz() {
        return clazz;
    }

    /** */
    public LoadTestConfig setClazz(Class<? extends Worker> clazz) {
        this.clazz = clazz;
        return this;
    }

    /** */
    public String getTestName() {
        return testName;
    }

    /** */
    public LoadTestConfig setTestName(String testName) {
        this.testName = testName;
        return this;
    }

    /** */
    public long getTestInitialPosition() {
        return testInitialPosition;
    }

    /** */
    public LoadTestConfig setTestInitialPosition(long testInitialPosition) {
        this.testInitialPosition = testInitialPosition;
        return this;
    }

    /** */
    public boolean isGradualLoadIncreaseEnabled() {
        return gradualLoadIncreaseEnabled;
    }

    /** */
    public LoadTestConfig setGradualLoadIncreaseEnabled(boolean gradualLoadIncreaseEnabled) {
        this.gradualLoadIncreaseEnabled = gradualLoadIncreaseEnabled;
        return this;
    }

    /** */
    public long getGradualLoadIncreasePeriodicity() {
        return gradualLoadIncreasePeriodicity;
    }

    /** */
    public LoadTestConfig setGradualLoadIncreasePeriodicity(long gradualLoadIncreasePeriodicity) {
        this.gradualLoadIncreasePeriodicity = gradualLoadIncreasePeriodicity;
        return this;
    }

    /** */
    public int getTestClientsNumber() {
        return testClientsNumber;
    }

    /** */
    public LoadTestConfig setTestClientsNumber(int testClientsNumber) {
        this.testClientsNumber = testClientsNumber;
        return this;
    }

}
