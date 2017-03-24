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

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteRunnable;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/11/2017 4:44 PM
 */
class UpdateStatisticsTask extends ActiveStoreIgniteRunnable {
    private final UUID localNodeId;
    private final int retries;
    private final int workersStarted;
    private final List<Long> operationDurations;
    private final List<OperationBoundTime> operationBoundTimes;

    @Inject
    private transient StatisticsCollector collector;

    public UpdateStatisticsTask(
        UUID localNodeId,
        int retries,
        int workersStarted,
        List<Long> operationDurations,
        List<OperationBoundTime> operationBoundTimes
    ) {
        this.localNodeId = localNodeId;
        this.retries = retries;
        this.workersStarted = workersStarted;
        this.operationDurations = operationDurations;
        this.operationBoundTimes = operationBoundTimes;
    }

    @Override public void runInjected() {
        collector.recordStatisticsUpdate(localNodeId, retries, workersStarted, operationDurations, operationBoundTimes);
    }
}
