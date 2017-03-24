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

package org.apache.ignite.load.simulation.main;

import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.ignite.Ignite;
import org.apache.ignite.load.LoadTestConfig;
import org.apache.ignite.load.Worker;
import org.apache.ignite.load.simulation.SimulationLoadTestDriver;
import org.apache.ignite.load.simulation.SimulationUtil;
import org.apache.ignite.load.simulation.TransactionDataGenerator;
import org.apache.ignite.load.statistics.Statistics;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/12/2017 4:43 PM
 */
public class MainLoadTestDriver extends SimulationLoadTestDriver {
    @Inject
    public MainLoadTestDriver(
        Ignite ignite,
        Statistics stats,
        LoadTestConfig config,
        Provider<? extends Worker> workersProvider
    ) {
        super(ignite, stats, config, workersProvider);
    }


    @Override public void setupDriver() throws Exception {
        SimulationUtil.prepopulateSimulationCaches(ignite);
        long txIdCounterStart = SimulationUtil.getNextTransactionsIdsBucketStart(ignite);
        TransactionDataGenerator.setTransactionIdCounterStart(txIdCounterStart);
    }
}
