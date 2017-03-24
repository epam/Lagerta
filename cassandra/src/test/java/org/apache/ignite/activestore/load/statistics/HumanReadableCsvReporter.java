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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.ignite.activestore.load.TestsHelper;
import org.apache.log4j.Logger;

/**
 * @author Evgeniy_Ignatiev
 * @since 10/7/2016 6:17 PM
 */
public class HumanReadableCsvReporter extends ScheduledReporter {
    /** */
    private final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

    /**
     * Returns a new {@link Builder} for {@link HumanReadableCsvReporter}.
     *
     * @param registry
     *     the registry to report
     *
     * @return a {@link Builder} instance for a {@link HumanReadableCsvReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link HumanReadableCsvReporter} instances. Defaults to using the default locale, converting
     * rates to events/second, converting durations to milliseconds, and not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Locale locale;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.locale = Locale.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.clock = Clock.defaultClock();
            this.filter = MetricFilter.ALL;
        }

        /**
         * Format numbers for the given {@link Locale}.
         *
         * @param locale
         *     a {@link Locale}
         *
         * @return {@code this}
         */
        public Builder formatFor(Locale locale) {
            this.locale = locale;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit
         *     a unit of time
         *
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit
         *     a unit of time
         *
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock
         *     a {@link Clock} instance
         *
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter
         *     a {@link MetricFilter}
         *
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Builds a {@link HumanReadableCsvReporter} with the given properties, writing {@code .csv} files to the
         * given directory.
         *
         * @param directory
         *     the directory in which the {@code .csv} files will be created
         *
         * @return a {@link HumanReadableCsvReporter}
         */
        public HumanReadableCsvReporter build(File directory) {
            return new HumanReadableCsvReporter(registry,
                directory,
                locale,
                rateUnit,
                durationUnit,
                clock,
                filter);
        }
    }

    /** */
    private static final Logger LOGGER = Logger.getLogger(HumanReadableCsvReporter.class);

    /** */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /** */
    private final File directory;

    /** */
    private final Locale locale;

    /** */
    private final Clock clock;

    private long testStartTime = -1;

    /** */
    private HumanReadableCsvReporter(MetricRegistry registry, File directory, Locale locale, TimeUnit rateUnit,
        TimeUnit durationUnit, Clock clock, MetricFilter filter) {
        super(registry, "human-readable-csv-reporter", filter, rateUnit, durationUnit);
        this.directory = directory;
        this.locale = locale;
        this.clock = clock;
    }

    /** {@inheritDoc} */
    @Override public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
        SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        String timestamp = timestampFormat.format(new Date(clock.getTime()));

        if (testStartTime < 0) {
            testStartTime = System.currentTimeMillis();
        }
        if (System.currentTimeMillis() - testStartTime < TestsHelper.getLoadTestsWarmupPeriod()) {
             return; // Start reporting only after at least one client completes warmup.
        }
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            reportGauge(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            reportCounter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            reportHistogram(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMeter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            reportTimer(timestamp, entry.getKey(), entry.getValue());
        }
    }

    /** */
    private void reportTimer(String timestamp, String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();

        report(timestamp,
            name,
            "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit",
            "%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,calls/%s,%s",
            timer.getCount(),
            convertDuration(snapshot.getMax()),
            convertDuration(snapshot.getMean()),
            convertDuration(snapshot.getMin()),
            convertDuration(snapshot.getStdDev()),
            convertDuration(snapshot.getMedian()),
            convertDuration(snapshot.get75thPercentile()),
            convertDuration(snapshot.get95thPercentile()),
            convertDuration(snapshot.get98thPercentile()),
            convertDuration(snapshot.get99thPercentile()),
            convertDuration(snapshot.get999thPercentile()),
            convertRate(timer.getMeanRate()),
            convertRate(timer.getOneMinuteRate()),
            convertRate(timer.getFiveMinuteRate()),
            convertRate(timer.getFifteenMinuteRate()),
            getRateUnit(),
            getDurationUnit());
    }

    /** */
    private void reportMeter(String timestamp, String name, Meter meter) {
        report(timestamp,
            name,
            "count,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit",
            "%d,%f,%f,%f,%f,events/%s",
            meter.getCount(),
            convertRate(meter.getMeanRate()),
            convertRate(meter.getOneMinuteRate()),
            convertRate(meter.getFiveMinuteRate()),
            convertRate(meter.getFifteenMinuteRate()),
            getRateUnit());
    }

    /** */
    private void reportHistogram(String timestamp, String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();

        report(timestamp,
            name,
            "count,max,mean,min,stddev,p50,p75,p90,p95,p98,p99,p999",
            "%d,%d,%f,%d,%f,%f,%f,%f,%f,%f,%f,%f",
            histogram.getCount(),
            snapshot.getMax(),
            snapshot.getMean(),
            snapshot.getMin(),
            snapshot.getStdDev(),
            snapshot.getMedian(),
            snapshot.get75thPercentile(),
            snapshot.getValue(0.9), // Add 90-th percentile to report.
            snapshot.get95thPercentile(),
            snapshot.get98thPercentile(),
            snapshot.get99thPercentile(),
            snapshot.get999thPercentile());
    }

    /** */
    private void reportCounter(String timestamp, String name, Counter counter) {
        report(timestamp, name, "count", "%d", counter.getCount());
    }

    /** */
    private void reportGauge(String timestamp, String name, Gauge gauge) {
        report(timestamp, name, "value", "%s", gauge.getValue());
    }

    /** */
    private void report(String timestamp, String name, String header, String line, Object... values) {
        final File file = new File(directory, sanitize(name) + ".csv");
        try {
            final boolean fileAlreadyExists = file.exists();

            if (!fileAlreadyExists) {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }
            final PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file, true), UTF_8));
            try {
                if (!fileAlreadyExists) {
                    out.println("t," + header);
                }
                out.printf(locale, String.format(locale, "%s,%s%n", timestamp, line), values);
            }
            finally {
                out.close();
            }
        }
        catch (IOException e) {
            LOGGER.warn("Error writing to " + file.getPath(), e);
        }
    }

    /** */
    protected String sanitize(String name) {
        return name;
    }
}
