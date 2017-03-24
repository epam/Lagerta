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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.ganglia.GangliaReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.apache.ignite.Ignite;

/**
 * Collects and reports statistics using metrics library.
 */
public class StatisticsCollector {
    /** Name of a {@link com.codahale.metrics.Timer} used to aggregate local node statistics. */
    static final String LATENCY_HISTOGRAM = "latency";

    /** */
    static final String THROUGHPUT_METER = "throughput";

    /** */
    static final String LOAD_THREADS_COUNTER = "load-threads";

    /** */
    private static final String RETRY_COUNTER = "retries";

    /** */
    private static final String GANGLIA_METRICS_PREFIX = "load_test";


    /** */
    private final MetricRegistry registry = new MetricRegistry();

    /** Frequency at which reporters should operate. */
    private final long reportFrequency;

    /** */
    StatisticsCollector(long reportFrequency) {
        this.reportFrequency = reportFrequency;
    }

    /**
     * Updates metrics with the latency of the recently completed operation.
     *
     * @param duration Latency of the recent operation in milliseconds.
     */
    public void recordOperationLatency(long duration) {
        registry.histogram(LATENCY_HISTOGRAM).update(duration);
    }

    /** */
    public void recordOperationsBatch(long batchSize) {
        registry.meter(THROUGHPUT_METER).mark(batchSize);
    }

    /** */
    public void recordOperationsRetries(int retries) {
        registry.counter(RETRY_COUNTER).inc(retries);
    }

    /** */
    public void recordWorkersThreadStarted(int workers) {
        registry.counter(LOAD_THREADS_COUNTER).inc(workers);
    }

    /**
     * Will report to ignite that node is overloaded if the latency for requests from the given quantile will become
     * larger than a given threshold.
     *
     * @param ignite Local ignite instance to be used for overload reporting.
     * @param latencyThreshold Hard threshold after exceeding which node will report overload.
     * @param quantile A quantile in {@code [0..1]}.
     */
    public void enableIgniteNodeOverloadStop(Ignite ignite, long latencyThreshold, double quantile) {
        ScheduledReporter reporter = IgniteNodeOverloadReporter.forRegistry(registry)
            .setIgnite(ignite)
            .setLatencyThreshold(latencyThreshold)
            .setQuantile(quantile)
            .build();
        reporter.start(reportFrequency, TimeUnit.MILLISECONDS);
    }

    /**
     * Enables statistics reporting to the console.
     */
    public void enableDebugReporting() {
        ScheduledReporter reporter = ConsoleReporter.forRegistry(registry)
            .build();
        reporter.start(reportFrequency, TimeUnit.MILLISECONDS);
    }

    /**
     * Enables statistics reporting to the csv file.
     *
     * @param csvReportDirectory Directory where .csv files for individual performance meters will be stored.
     */
    public void enableCsvReporting(String csvReportDirectory) {
        ScheduledReporter reporter = HumanReadableCsvReporter.forRegistry(registry)
            .build(new File(csvReportDirectory));
        reporter.start(reportFrequency, TimeUnit.MILLISECONDS);
    }

    /** */
    public void enableGangliaReporting(InetSocketAddress gangliaAddress) throws IOException {
        GMetric ganglia = new GMetric(gangliaAddress.getHostString(), gangliaAddress.getPort(),
            GMetric.UDPAddressingMode.UNICAST, 1);
        GangliaReporter reporter = GangliaReporter.forRegistry(registry)
            .prefixedWith(GANGLIA_METRICS_PREFIX)
            .build(ganglia);
        reporter.start(reportFrequency, TimeUnit.MILLISECONDS);
    }
}
