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

package org.apache.ignite.activestore.load.simulation;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.load.TestsHelper;
import org.apache.ignite.activestore.utils.CassandraHelper;
import org.apache.log4j.Logger;

/**
 * Load test that simulates work of a bank transaction processing system, where each transaction is a money transfer
 * from one account to another and it can only be performed if the source account has enough money.
 */
public class SimulationLoadTest {
    /** */
    private static final Logger LOGGER = Logger.getLogger("SimulationLoadTest");

    /** */
    public static void main(String[] args) {
        Ignite ignite = TestsHelper.getClusterClient();

        try {
            LOGGER.info("Simulation load test execution started");

            SimulationUtil.prepopulateSimulationCaches(ignite);
            long txIdCounterStart = SimulationUtil.getNextTransactionsIdsBucketStart(ignite);
            TransactionDataGenerator.setTransactionIdCounterStart(txIdCounterStart);
            SimulationLoadTestDriver.runSimulation(LOGGER, ignite);

            LOGGER.info("Simulation load test execution completed");
        }
        catch (Throwable e) {
            LOGGER.error("Simulation load test execution failed", e);
            throw new RuntimeException("Simulation load test execution failed", e);
        }
        finally {
            if (ignite != null) {
                ignite.close();
            }
            CassandraHelper.releaseCassandraResources();
        }
    }
}
