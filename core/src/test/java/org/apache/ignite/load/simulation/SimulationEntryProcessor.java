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

package org.apache.ignite.load.simulation;

import java.util.Map;

import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.load.WorkerEntryProcessor;
import org.apache.ignite.load.statistics.Statistics;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/23/2017 2:56 PM
 */
public class SimulationEntryProcessor implements WorkerEntryProcessor {
    private final Ignite ignite;
    private final Statistics stats;

    @Inject
    public SimulationEntryProcessor(Ignite ignite, Statistics stats) {
        this.ignite = ignite;
        this.stats = stats;
    }

    /** {@inheritDoc} */
    @Override public void process(Map<?, ?> entries) {
        // ToDo: Maybe we need to report average statistics for the whole batch?
        for (Object value : entries.values()) {
            long transactionStartTime = System.currentTimeMillis();
            SimulationUtil.processTransaction(ignite, (TransactionData)value);
            stats.recordOperation(System.currentTimeMillis() - transactionStartTime);
        }
    }
}
