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

/**
 * @author Evgeniy_Ignatiev
 * @since 1/24/2017 6:35 PM
 */
public abstract class AdvancedReporter extends ScheduledReporter {
    private final long warmupDuration;

    private long firstReportTime = -1;

    protected AdvancedReporter(MetricRegistry registry, String name, long warmupDuration) {
        super(registry, name, MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
        this.warmupDuration = warmupDuration;
    }

    @Override public void report(
        SortedMap<String, Gauge> gauges,
        SortedMap<String, Counter> counters,
        SortedMap<String, Histogram> histograms,
        SortedMap<String, Meter> meters,
        SortedMap<String, Timer> timers
    ) {
        if (firstReportTime < 0) {
            firstReportTime = System.currentTimeMillis();
        }
        if (System.currentTimeMillis() - firstReportTime < warmupDuration) {
            return;
        }
        reportStatistics(gauges, counters, histograms, meters, timers);
    }

    protected abstract void reportStatistics(
        SortedMap<String, Gauge> gauges,
        SortedMap<String, Counter> counters,
        SortedMap<String, Histogram> histograms,
        SortedMap<String, Meter> meters,
        SortedMap<String, Timer> timers
    );
}

