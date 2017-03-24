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

import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.load.Generator;
import org.apache.ignite.activestore.load.LoadTestConfig;
import org.apache.ignite.activestore.load.Worker;
import org.apache.ignite.activestore.load.WorkerEntryProcessor;
import org.apache.ignite.activestore.load.statistics.Statistics;

/**
 * {@link Worker} implementation that tests performance of simulation transactions each of which consists of performing
 * three get and three put operations.
 */
public class SimulationWorker extends Worker {
    /** */
    public static final String LOGGER_NAME = "SimulationWorker";

    /** */
    public SimulationWorker(Ignite ignite, LoadTestConfig config, long startPosition, long endPosition,
        Generator keyGenerator, Generator valueGenerator) {
        super(ignite, config, startPosition, endPosition, keyGenerator, valueGenerator);
    }

    /** {@inheritDoc} */
    @Override protected String loggerName() {
        return LOGGER_NAME;
    }

    /** {@inheritDoc} */
    @Override protected WorkerEntryProcessor getEntryProcessor(Ignite ignite, LoadTestConfig config) {
        return new SimulationEntryProcessor(ignite);
    }

    /** */
    private static class SimulationEntryProcessor implements WorkerEntryProcessor {
        /** */
        private final Ignite ignite;

        /** */
        SimulationEntryProcessor(Ignite ignite) {
            this.ignite = ignite;
        }

        /** {@inheritDoc} */
        @Override public void process(Map<?, ?> entries) {
            // ToDo: Maybe we need to report average statistics for the whole batch?
            for (Object value : entries.values()) {
                long transactionStartTime = System.currentTimeMillis();
                SimulationUtil.processTransaction(ignite, (TransactionData)value);
                Statistics.recordOperation(System.currentTimeMillis() - transactionStartTime);
            }
        }
    }
}
