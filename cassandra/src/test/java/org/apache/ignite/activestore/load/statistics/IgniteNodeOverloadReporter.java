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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.load.ClusterStopper;
import org.apache.ignite.activestore.load.TestsHelper;
import org.apache.log4j.Logger;

/**
 * Reporter that detects by using performance metrics if the ignite node is overloaded with requests and signal the
 * overload to remote observers.
 */
public class IgniteNodeOverloadReporter extends ScheduledReporter {
    /** */
    private static final Logger LOGGER = Logger.getLogger(IgniteNodeOverloadReporter.class);

    /** */
    private static final int MINIMUM_NUMBER_OF_MEASUREMENTS = 5;

    /** */
    private final long latencyThreshold;

    /** */
    private final double quantile;

    /** */
    private final Ignite ignite;

    /** */
    private boolean warmup = true;

    /** */
    private long startTime;

    /** */
    private Long workers;

    /** */
    private final List<Double> measuredLatencies = new ArrayList<>();

    /** */
    private boolean clusterOverloaded = false;

    /** */
    private IgniteNodeOverloadReporter(MetricRegistry registry, TimeUnit rateUnit, TimeUnit durationUnit,
        MetricFilter filter, long latencyThreshold, double quantile, Ignite ignite
    ) {
        super(registry, "ignite-overload-reporter", filter, rateUnit, durationUnit);
        this.latencyThreshold = latencyThreshold;
        this.quantile = quantile;
        this.ignite = ignite;
    }

    /** */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /** {@inheritDoc} */
    @Override public void start(long period, TimeUnit unit) {
        super.start(period, unit);
    }

    /** {@inheritDoc} */
    @Override public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
        SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
        SortedMap<String, Timer> timers) {
        if (clusterOverloaded) {
            return;
        }
        Histogram histogram = histograms.get(StatisticsCollector.LATENCY_HISTOGRAM);
        Counter workersCounter = counters.get(StatisticsCollector.LOAD_THREADS_COUNTER);

        if (histogram != null) {
            if (startTime == 0) {
                // Set start time only when load testing has been started.
                startTime = System.currentTimeMillis();
            }
            if (warmup && ((System.currentTimeMillis() - startTime) >= TestsHelper.getLoadTestsWarmupPeriod())) {
                warmup = false;
            }
            if (warmup) {
                return; // Exclude warmup period from overload stop checking.
            }
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
                        LOGGER.warn("Cluster is considered overloaded and will be stopped.");
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

    /** */
    public static class Builder {
        private final MetricRegistry registry;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private long latencyThreshold;
        private double quantile;
        private Ignite ignite;

        /** */
        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /** */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /** */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /** */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /** */
        public Builder setLatencyThreshold(long latencyThreshold) {
            this.latencyThreshold = latencyThreshold;
            return this;
        }

        /** */
        public Builder setQuantile(double quantile) {
            this.quantile = quantile;
            return this;
        }

        /** */
        public Builder setIgnite(Ignite ignite) {
            this.ignite = ignite;
            return this;
        }

        /** */
        public IgniteNodeOverloadReporter build() {
            return new IgniteNodeOverloadReporter(registry, rateUnit, durationUnit, filter, latencyThreshold,
                quantile, ignite);
        }
    }
}
