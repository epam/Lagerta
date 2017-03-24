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

package org.apache.ignite.load;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;

/**
 * Helper class for load tests. Used to simplify configuration processing.
 */
public class TestsHelper {
    private static final PropertiesParser SETTINGS = new PropertiesParser("/load/load-tests.properties");

    /** */
    private static final int BULK_OPERATION_SIZE = SETTINGS.parseIntSetting("bulk.operation.size");

    /** */
    private static final String LOAD_TESTS_CACHE_NAME = SETTINGS.get("load.tests.cache.name");

    /** */
    private static final int LOAD_TESTS_THREADS_COUNT = SETTINGS.parseIntSetting("load.tests.threads.count");

    /** */
    private static final long LOAD_TESTS_WARMUP_PERIOD = SETTINGS.parseLongSetting("load.tests.warmup.period");

    /** */
    private static final long LOAD_TESTS_EXECUTION_TIME = SETTINGS.parseLongSetting("load.tests.execution.time");

    /** */
    private static final long LOAD_TESTS_REQUESTS_LATENCY = SETTINGS.parseLongSetting("load.tests.requests.latency");

    /** */
    private static final int LOAD_TESTS_CLIENTS_NUMBER = SETTINGS.parseIntSetting("load.tests.clients.number");

    /** */
    private static final boolean LOAD_TESTS_GRADUAL_LOAD_INCREASE_ENABLED = SETTINGS.parseBoolean(
        "load.tests.gradual.load.increase.enabled");

    private static final String LOAD_TESTS_TEST_NAME = SETTINGS.get("load.tests.test.name");

    /** */
    private static final long LOAD_TESTS_GRADUAL_LOAD_INCREASE_PERIODICITY = SETTINGS.parseLongSetting(
        "load.tests.gradual.load.increase.periodicity");

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

    public static Ignite getClusterClient(String[] args) {
        return Ignition.start(new IgniteConfiguration(getIgniteConfigFromCommandLineArgs(args)) {{
            setClientMode(true);
        }});
    }

    public static Ignite getClusterServer(String[] args) {
        return Ignition.start(getIgniteConfigFromCommandLineArgs(args));
    }

    private static IgniteConfiguration getIgniteConfigFromCommandLineArgs(String[] args) {
        if (args.length < 1) {
            throw new RuntimeException("Xml config path not passed");
        }
        ApplicationContext appContext = new GenericXmlApplicationContext(args[0]);
        return appContext.getBean(IgniteConfiguration.class);
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
    public static int getLoadTestsClientsNumber() {
        return LOAD_TESTS_CLIENTS_NUMBER;
    }

    public static String getLoadTestsTestName() {
        return LOAD_TESTS_TEST_NAME;
    }

    public static double calculateErrorPercent(long count, long errorCount) {
        return count + errorCount == 0 ? 0 : (double)(errorCount * 100) / (double)(count + errorCount);
    }
}
