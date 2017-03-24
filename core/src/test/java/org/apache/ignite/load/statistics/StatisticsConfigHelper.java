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

import org.apache.ignite.load.PropertiesParser;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/20/2017 7:33 PM
 */
public final class StatisticsConfigHelper {
    private static final PropertiesParser SETTINGS = new PropertiesParser("/load/statistics.properties");

    private static final long REPORT_FREQUENCY = SETTINGS.parseLongSetting("statistics.report.frequency");

    private static final boolean DEBUG_REPORTING = SETTINGS.parseBoolean("statistics.debug.reporting");

    private static final boolean OVERLOAD_STOP = SETTINGS.parseBoolean("statistics.overload.stop");

    private static final long LATENCY_THRESHOLD = SETTINGS.parseLongSetting("statistics.latency.threshold");

    private static final double LATENCY_THRESHOLD_QUANTILE = SETTINGS.parseDoubleSetting(
        "statistics.latency.threshold.quantile");

    private static final boolean AGGREGATED_REPORTING = SETTINGS.parseBoolean("statistics.aggregated.reporting");

    private static final String AGGREGATED_REPORT_LOCATION = SETTINGS.get(
        "statistics.aggregated.report.location");

    private static final boolean GANGLIA_REPORTING = SETTINGS.parseBoolean("statistics.ganglia.reporting");

    private static final String GANGLIA_HOST = SETTINGS.get("statistics.ganglia.host");

    private static final int GANGLIA_PORT = SETTINGS.parseIntSetting("statistics.ganglia.port");

    private static final long WARMUP_DURATION = SETTINGS.parseLongSetting("statistics.warmup.skip.duration");

    private StatisticsConfigHelper() {
    }

    public static long getReportFrequency() {
        return REPORT_FREQUENCY;
    }

    public static boolean isDebugReportingEnabled() {
        return DEBUG_REPORTING;
    }

    public static boolean isNodeOverloadStopEnabled() {
        return OVERLOAD_STOP;
    }

    public static long getLatencyThreshold() {
        return LATENCY_THRESHOLD;
    }

    public static double getLatencyThresholdQuantile() {
        return LATENCY_THRESHOLD_QUANTILE;
    }

    public static boolean isAggregatedReportEnabled() {
        return AGGREGATED_REPORTING;
    }

    public static String getAggregatedReportLocation() {
        return AGGREGATED_REPORT_LOCATION.replace("\n", "").replace("\r", "");
    }

    public static boolean isGangliaReportingEnabled() {
        return GANGLIA_REPORTING;
    }

    public static InetSocketAddress getGangliaAddress() {
        return new InetSocketAddress(GANGLIA_HOST, GANGLIA_PORT);
    }

    public static long getWarmupDuration() {
        return WARMUP_DURATION;
    }
}
