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

import java.net.InetSocketAddress;
import java.util.ResourceBundle;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;

/**
 * Helper class for load tests. Used to simplify configuration processing.
 */
public class TestsHelper {
    /** */
    private static final ResourceBundle TESTS_SETTINGS = ResourceBundle.getBundle("load-tests");

    /** */
    private static final int BULK_OPERATION_SIZE = parseIntSetting("bulk.operation.size");

    /** */
    private static final String LOAD_TESTS_CACHE_NAME = TESTS_SETTINGS.getString("load.tests.cache.name");

    /** */
    private static final int LOAD_TESTS_THREADS_COUNT = parseIntSetting("load.tests.threads.count");

    /** */
    private static final long LOAD_TESTS_WARMUP_PERIOD = parseLongSetting("load.tests.warmup.period");

    /** */
    private static final long LOAD_TESTS_EXECUTION_TIME = parseLongSetting("load.tests.execution.time");

    /** */
    private static final long LOAD_TESTS_REQUESTS_LATENCY = parseLongSetting("load.tests.requests.latency");

    /** */
    private static final long LOAD_TESTS_STATISTICS_REPORT_FREQUENCY = parseLongSetting("load.tests.statistics.report.frequency");

    /** */
    private static final String LOAD_TESTS_CONFIG = TESTS_SETTINGS.getString("load.tests.config");

    /** */
    private static final boolean LOAD_TESTS_STATISTICS_DEBUG_REPORTING = parseSwitch("load.tests.statistics.debug.reporting");

    /** */
    private static final boolean LOAD_TESTS_STATISTICS_OVERLOAD_STOP = parseSwitch("load.tests.statistics.overload.stop");

    /** */
    private static final long LOAD_TESTS_STATISTICS_LATENCY_THRESHOLD = parseLongSetting("load.tests.statistics.latency.threshold");

    /** */
    private static final double LOAD_TESTS_STATISTICS_LATENCY_THRESHOLD_QUANTILE = parseDoubleSetting("load.tests.statistics.latency.threshold.quantile");

    /** */
    private static final boolean LOAD_TESTS_STATISTICS_AGGREGATED_REPORTING = parseSwitch("load.tests.statistics.aggregated.reporting");

    /** */
    private static final String LOAD_TESTS_STATISTICS_AGGREGATED_REPORT_LOCATION = TESTS_SETTINGS.getString("load.tests.statistics.aggregated.report.location");

    /** */
    private static final int LOAD_TESTS_CLIENTS_NUMBER = parseIntSetting("load.tests.clients.number");

    /** */
    private static final Generator LOAD_TESTS_KEY_GENERATOR;

    /** */
    private static final Generator LOAD_TESTS_VALUE_GENERATOR;

    /** */
    private static final ApplicationContext LOAD_TESTS_APP_CONTEXT;

    /** */
    private static final boolean LOAD_TESTS_GRADUAL_LOAD_INCREASE_ENABLED = parseSwitch("load.tests.gradual.load.increase.enabled");

    /** */
    private static final long LOAD_TESTS_GRADUAL_LOAD_INCREASE_PERIODICITY = parseLongSetting("load.tests.gradual.load.increase.periodicity");

    /** */
    private static final boolean LOAD_TESTS_STATISTICS_GANGLIA_REPORTING = parseSwitch("load.tests.statistics.ganglia.reporting");

    /** */
    private static final String LOAD_TESTS_STATISTICS_GANGLIA_HOST = TESTS_SETTINGS.getString("load.tests.statistics.ganglia.host");

    /** */
    private static final int LOAD_TESTS_STATISTICS_GANGLIA_PORT = parseIntSetting("load.tests.statistics.ganglia.port");

    static {
        try {
            LOAD_TESTS_KEY_GENERATOR = (Generator)Class.forName(TESTS_SETTINGS.getString("load.tests.key.generator")).newInstance();
            LOAD_TESTS_VALUE_GENERATOR = (Generator)Class.forName(TESTS_SETTINGS.getString("load.tests.value.generator")).newInstance();
            LOAD_TESTS_APP_CONTEXT = new GenericXmlApplicationContext(LOAD_TESTS_CONFIG);
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to initialize TestsHelper", e);
        }
    }

    /** */
    private static int parseIntSetting(String name) {
        return Integer.parseInt(TESTS_SETTINGS.getString(name));
    }

    /** */
    private static long parseLongSetting(String name) {
        return Long.parseLong(TESTS_SETTINGS.getString(name));
    }

    /** */
    private static double parseDoubleSetting(String name) {
        return Double.parseDouble(TESTS_SETTINGS.getString(name));
    }

    /** */
    private static boolean parseSwitch(String name) {
        return Boolean.parseBoolean(TESTS_SETTINGS.getString(name));
    }

    /** */
    public static int getLoadTestsThreadsCount() {
        return LOAD_TESTS_THREADS_COUNT;
    }

    /** */
    public static long getLoadTestsWarmupPeriod() {
        return LOAD_TESTS_WARMUP_PERIOD;
    }

    /** */
    public static long getLoadTestsExecutionTime() {
        return LOAD_TESTS_EXECUTION_TIME;
    }

    /** */
    public static long getLoadTestsRequestsLatency() {
        return LOAD_TESTS_REQUESTS_LATENCY;
    }

    /** */
    public static int getBulkOperationSize() {
        return BULK_OPERATION_SIZE;
    }

    /** */
    public static String getLoadTestsCacheName() {
        return LOAD_TESTS_CACHE_NAME;
    }

    /** */
    public static Generator getLoadTestsKeyGenerator() {
        return LOAD_TESTS_KEY_GENERATOR;
    }

    /** */
    public static Generator getLoadTestsValueGenerator() {
        return LOAD_TESTS_VALUE_GENERATOR;
    }

    /** */
    public static long getLoadTestsStatisticsReportFrequency() {
        return LOAD_TESTS_STATISTICS_REPORT_FREQUENCY;
    }

    /** */
    public static boolean isLoadTestsStatisticsDebugReporEnabled() {
        return LOAD_TESTS_STATISTICS_DEBUG_REPORTING;
    }

    /** */
    public static boolean isLoadTestsStatisticsOverloadStopEnabled() {
        return LOAD_TESTS_STATISTICS_OVERLOAD_STOP;
    }

    /** */
    public static long getLoadTestsStatisticsLatencyThreshold() {
        return LOAD_TESTS_STATISTICS_LATENCY_THRESHOLD;
    }

    /** */
    public static double getLoadTestsStatisticsLatencyThresholdQuantile() {
        return LOAD_TESTS_STATISTICS_LATENCY_THRESHOLD_QUANTILE;
    }

    /** */
    public static boolean isLoadTestsStatisticsAggregatedReportEnabled() {
        return LOAD_TESTS_STATISTICS_AGGREGATED_REPORTING;
    }

    /** */
    public static String getLoadTestsStatisticsAggregatedReportLocation() {
        return LOAD_TESTS_STATISTICS_AGGREGATED_REPORT_LOCATION.replace("\n", "").replace("\r", "");
    }

    /** */
    public static Ignite getClusterClient() {
        return Ignition.start(new IgniteConfiguration(getLoadTestsIgniteConfig()) {{
            setClientMode(true);
        }});
    }

    /** */
    public static IgniteConfiguration getLoadTestsIgniteConfig() {
        return LOAD_TESTS_APP_CONTEXT.getBean(IgniteConfiguration.class);
    }

    /** */
    public static boolean isLoadTestsGradualLoadIncreaseEnabled() {
        return LOAD_TESTS_GRADUAL_LOAD_INCREASE_ENABLED;
    }

    /** */
    public static long getLoadTestsGradualLoadIncreasePeriodicity() {
        return LOAD_TESTS_GRADUAL_LOAD_INCREASE_PERIODICITY;
    }

    /** */
    public static boolean isLoadTestsStatisticsGangliaReportingEnabled() {
        return LOAD_TESTS_STATISTICS_GANGLIA_REPORTING;
    }

    /** */
    public static InetSocketAddress getLoadTestsStatisticsGangliaAddress() {
        return new InetSocketAddress(LOAD_TESTS_STATISTICS_GANGLIA_HOST, LOAD_TESTS_STATISTICS_GANGLIA_PORT);
    }

    /** */
    public static int getLoadTestsClientsNumber() {
        return LOAD_TESTS_CLIENTS_NUMBER;
    }
}
