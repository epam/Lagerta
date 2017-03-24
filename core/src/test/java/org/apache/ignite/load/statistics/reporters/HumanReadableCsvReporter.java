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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;

/**
 * @author Evgeniy_Ignatiev
 * @since 10/7/2016 6:17 PM
 */
public class HumanReadableCsvReporter extends AdvancedReporter {
    private final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
    private static final Logger LOGGER = LoggerFactory.getLogger(HumanReadableCsvReporter.class);
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private final File directory;
    private final Locale locale = Locale.getDefault();
    private final Clock clock = Clock.defaultClock();

    public HumanReadableCsvReporter(MetricRegistry registry, long warmupDuration, File directory) {
        super(registry, "human-readable-csv-reporter", warmupDuration);
        this.directory = directory;
    }

    /** {@inheritDoc} */
    @Override public void reportStatistics(
        SortedMap<String, Gauge> gauges,
        SortedMap<String, Counter> counters,
        SortedMap<String, Histogram> histograms,
        SortedMap<String, Meter> meters,
        SortedMap<String, Timer> timers
    ) {
        String timestamp = timestampFormat.format(new Date(clock.getTime()));

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
            try (PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file, true), UTF_8))) {
                if (!fileAlreadyExists) {
                    out.println("t," + header);
                }
                out.printf(locale, String.format(locale, "%s,%s%n", timestamp, line), values);
            }
        }
        catch (IOException e) {
            LOGGER.warn("[T] Error writing to " + file.getPath(), e);
        }
    }

    private String sanitize(String name) {
        return name;
    }
}
