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

package org.apache.ignite.load.subscriber;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.cluster.XmlOneProcessClusterManager;
import org.apache.ignite.activestore.commons.injection.Injection;
import org.apache.ignite.activestore.impl.subscriber.lead.Lead;
import org.apache.ignite.activestore.rules.TestResources;
import org.apache.ignite.load.statistics.StatisticsDriver;
import org.apache.ignite.load.statistics.reporters.ReportersManager;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/24/2017 12:36 PM
 */
public class BaseLeadLoadTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseLeadLoadTest.class);

    private static final String CONFIG_PATH = "load/lead-load-test-config.xml";
    private static final TestResources CLUSTER_RESOURCE = new TestResources("cluster", 1);
    private static final int TEST_TIMEOUT = 7 * 60_000;
    private static final long BOOTSTRAP_OVERHEAD = 2 * 60_000;

    public static final String WORKERS_BIND_NAME = "workers";
    public static final String REQUESTS_PERIOD_BIND_NAME = "requestsPeriod";

    public static final long REPORT_FREQUENCY = 30_000;
    public static final String REPORT_LOCATION = "report";
    public static final String CACHE_NAME = "cache";
    public static final long SLEEP_TIME = 100;
    public static final long TEST_EXECUTION_TIME = TEST_TIMEOUT - BOOTSTRAP_OVERHEAD;
    public static final long INITIAL_REQUESTS_PERIOD = 100;
    public static final long PERIOD_DECREASE_STEP = 10;
    public static final long MINIMUM_REQUESTS_PERIOD = 10;
    public static final long PERIOD_DECREASE_PERIOD = 60_000;
    public static final int BATCH_SIZE = 100;
    public static final long LATENCY_THRESHOLD = 100;
    public static final double LATENCY_QUANTILE = 0.9;
    public static final long WARMUP_DURATION = 10_000;
    public static final int ID_PATTERN_PERIOD = 20;

    static {
        CLUSTER_RESOURCE.setClusterManager(new XmlOneProcessClusterManager(CONFIG_PATH));
    }

    @ClassRule
    public static RuleChain allResourcesRule = RuleChain
        .outerRule(new Timeout(TEST_TIMEOUT))
        .around(CLUSTER_RESOURCE);

    @Inject
    private ReportersManager reportersManager;

    @Inject
    private Lead lead;

    @Inject
    private StatisticsDriver driver;

    @Before
    public void setUp() throws InterruptedException {
        Injection.inject(this, ignite());
        driver.startCollecting();
        reportersManager.startReporters();
        awaitLeadInitialization();
    }

    private void awaitLeadInitialization() throws InterruptedException {
        LOGGER.info("[T] Start waiting lead");
        while (!lead.isInitialized()) {
            Thread.sleep(1_000);
        }
        LOGGER.info("[T] Finish waiting lead");
    }

    protected static Ignite ignite() {
        return CLUSTER_RESOURCE.ignite();
    }
}
