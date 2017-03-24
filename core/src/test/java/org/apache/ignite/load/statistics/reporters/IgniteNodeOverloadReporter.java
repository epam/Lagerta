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

package org.apache.ignite.load.statistics.reporters;

import com.codahale.metrics.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.load.ClusterStopper;
import org.apache.ignite.load.statistics.StatisticsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Reporter that detects by using performance metrics if the ignite node is overloaded with requests and signal the
 * overload to remote observers.
 */
public class IgniteNodeOverloadReporter extends AdvancedReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteNodeOverloadReporter.class);
    private static final int MINIMUM_NUMBER_OF_MEASUREMENTS = 5;

    private final long latencyThreshold;
    private final double quantile;
    private final Ignite ignite;

    private final List<Double> measuredLatencies = new ArrayList<>();

    private Long workers;
    private boolean clusterOverloaded = false;

    public IgniteNodeOverloadReporter(
        MetricRegistry registry,
        long warmupDuration,
        long latencyThreshold,
        double quantile,
        Ignite ignite
    ) {
        super(registry, "ignite-overload-reporter", warmupDuration);
        this.latencyThreshold = latencyThreshold;
        this.quantile = quantile;
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public void start(long period, TimeUnit unit) {
        super.start(period, unit);
    }

    /** {@inheritDoc} */
    @Override public void reportStatistics(
        SortedMap<String, Gauge> gauges,
        SortedMap<String, Counter> counters,
        SortedMap<String, Histogram> histograms,
        SortedMap<String, Meter> meters,
        SortedMap<String, Timer> timers
    ) {
        if (clusterOverloaded) {
            return;
        }
        Histogram histogram = histograms.get(StatisticsCollector.LATENCY_HISTOGRAM);
        Counter workersCounter = counters.get(StatisticsCollector.LOAD_THREADS_COUNTER);

        if (histogram != null) {
            double currentLatency = histogram.getSnapshot().getValue(quantile);
            Long currentWorkers = workersCounter == null ? null : workersCounter.getCount();

            if (workers == null) {
                workers = currentWorkers;
            }
            if (workers != null && !workers.equals(currentWorkers)) {
                if (measuredLatencies.size() >= MINIMUM_NUMBER_OF_MEASUREMENTS) {
                    int medianIndex = measuredLatencies.size() / 2;

                    Collections.sort(measuredLatencies);
                    if (measuredLatencies.get(medianIndex) > latencyThreshold) {
                        clusterOverloaded = true;
                        LOGGER.warn("[T] Cluster is considered overloaded and will be stopped.");
                        ClusterStopper.stopCluster(ignite);
                        return;
                    }
                }
                measuredLatencies.clear();
                workers = currentWorkers;
            }
            if (workers == null || workers.equals(currentWorkers)) {
                measuredLatencies.add(currentLatency);
            }
        }
    }
}
