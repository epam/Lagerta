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

package org.apache.ignite.load.statistics;

import java.net.InetSocketAddress;

public class StatisticsConfig {
    private boolean debugReporting;
    private boolean nodeOverloadStop;
    private boolean csvReporting;
    private long reportFrequency;
    private long latencyThreshold;
    private double quantile;
    private String csvReportDirectory;
    private boolean gangliaReporting;
    private InetSocketAddress gangliaAddress;
    private long warmupDuration;

    public static StatisticsConfig defaultConfig() {
        return new StatisticsConfig()
            .enableDebugReporting(StatisticsConfigHelper.isDebugReportingEnabled())
            .enableNodeOverloadStop(StatisticsConfigHelper.isNodeOverloadStopEnabled())
            .setLatencyThreshold(StatisticsConfigHelper.getLatencyThreshold())
            .setQuantile(StatisticsConfigHelper.getLatencyThresholdQuantile())
            .setReportFrequency(StatisticsConfigHelper.getReportFrequency())
            .enableCsvReporting(StatisticsConfigHelper.isAggregatedReportEnabled())
            .setCsvReportDirectory(StatisticsConfigHelper.getAggregatedReportLocation())
            .enableGangliaReporting(StatisticsConfigHelper.isGangliaReportingEnabled())
            .setGangliaAddress(StatisticsConfigHelper.getGangliaAddress())
            .setWarmupDuration(StatisticsConfigHelper.getWarmupDuration());
    }

    public StatisticsConfig enableDebugReporting(boolean debugReporting) {
        this.debugReporting = debugReporting;
        return this;
    }

    public StatisticsConfig enableNodeOverloadStop(boolean nodeOverloadStop) {
        this.nodeOverloadStop = nodeOverloadStop;
        return this;
    }

    public StatisticsConfig enableCsvReporting(boolean csvReporting) {
        this.csvReporting = csvReporting;
        return this;
    }

    public StatisticsConfig setReportFrequency(long reportFrequency) {
        this.reportFrequency = reportFrequency;
        return this;
    }

    public StatisticsConfig setLatencyThreshold(long latencyThreshold) {
        this.latencyThreshold = latencyThreshold;
        return this;
    }

    public StatisticsConfig setQuantile(double quantile) {
        this.quantile = quantile;
        return this;
    }

    public StatisticsConfig setCsvReportDirectory(String csvReportDirectory) {
        this.csvReportDirectory = csvReportDirectory;
        return this;
    }

    public StatisticsConfig enableGangliaReporting(boolean gangliaReporting) {
        this.gangliaReporting = gangliaReporting;
        return this;
    }

    public StatisticsConfig setGangliaAddress(InetSocketAddress gangliaAddress) {
        this.gangliaAddress = gangliaAddress;
        return this;
    }

    public boolean isDebugReportingEnabled() {
        return debugReporting;
    }

    public boolean isNodeOverloadStopEnabled() {
        return nodeOverloadStop;
    }

    public boolean isCsvReportingEnabled() {
        return csvReporting;
    }

    public long getReportFrequency() {
        return reportFrequency;
    }

    public long getLatencyThreshold() {
        return latencyThreshold;
    }

    public double getQuantile() {
        return quantile;
    }

    public String getCsvReportDirectory() {
        return csvReportDirectory;
    }

    public boolean isGangliaReportingEnabled() {
        return gangliaReporting;
    }

    public InetSocketAddress getGangliaAddress() {
        return gangliaAddress;
    }

    public StatisticsConfig setWarmupDuration(long warmupDuration) {
        this.warmupDuration = warmupDuration;
        return this;
    }

    public long getWarmupDuration() {
        return warmupDuration;
    }
}
