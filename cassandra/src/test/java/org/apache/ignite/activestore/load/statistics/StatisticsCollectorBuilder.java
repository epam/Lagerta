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

package org.apache.ignite.activestore.load.statistics;

import org.apache.ignite.Ignite;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Builder of {@link StatisticsCollector} instances.
 */
public class StatisticsCollectorBuilder {
    /** */
    private boolean debugReporting;

    /** */
    private boolean nodeOverloadStop;

    /** */
    private boolean csvReporting;

    /** */
    private long reportFrequency;

    /** */
    private long latencyThreshold;

    /** */
    private double quantile;

    /** */
    private String csvReportDirectory;

    /** */
    private boolean gangliaReporting;

    /** */
    private InetSocketAddress gangliaAddress;

    /** */
    public StatisticsCollectorBuilder enableDebugReporting(boolean debugReporting) {
        this.debugReporting = debugReporting;
        return this;
    }

    /** */
    public StatisticsCollectorBuilder enableNodeOverloadStop(boolean nodeOverloadStop) {
        this.nodeOverloadStop = nodeOverloadStop;
        return this;
    }

    /** */
    public StatisticsCollectorBuilder enableCsvReporting(boolean csvReporting) {
        this.csvReporting = csvReporting;
        return this;
    }

    /** */
    public StatisticsCollectorBuilder setReportFrequency(long reportFrequency) {
        this.reportFrequency = reportFrequency;
        return this;
    }

    /** */
    public StatisticsCollectorBuilder setLatencyThreshold(long latencyThreshold) {
        this.latencyThreshold = latencyThreshold;
        return this;
    }

    /** */
    public StatisticsCollectorBuilder setQuantile(double quantile) {
        this.quantile = quantile;
        return this;
    }

    /** */
    public StatisticsCollectorBuilder setCsvReportDirectory(String csvReportDirectory) {
        this.csvReportDirectory = csvReportDirectory;
        return this;
    }

    /** */
    public StatisticsCollectorBuilder enableGangliaReporting(boolean gangliaReporting) {
        this.gangliaReporting = gangliaReporting;
        return this;
    }

    /** */
    public StatisticsCollectorBuilder setGangliaAddress(InetSocketAddress gangliaAddress) {
        this.gangliaAddress = gangliaAddress;
        return this;
    }

    /**
     * Builds a new instance of {@link StatisticsCollector}.
     */
    public StatisticsCollector build(Ignite ignite) throws IOException {
        StatisticsCollector collector = new StatisticsCollector(reportFrequency);

        if (csvReporting) {
            collector.enableCsvReporting(csvReportDirectory);
        }
        if (debugReporting) {
            collector.enableDebugReporting();
        }
        if (nodeOverloadStop) {
            collector.enableIgniteNodeOverloadStop(ignite, latencyThreshold, quantile);
        }
        if (gangliaReporting) {
            collector.enableGangliaReporting(gangliaAddress);
        }
        return collector;
    }
}
