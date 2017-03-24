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

/**
 * @author Evgeniy_Ignatiev
 * @since 1/26/2017 3:09 PM
 */
public class LocalStatisticsUpdater extends StatisticsUpdater {
    private final StatisticsCollector collector;

    public LocalStatisticsUpdater(
        UUID nodeId,
        Statistics stats,
        StatisticsCollector collector
    ) {
        super(nodeId, stats);
        this.collector = collector;
    }

    @Override protected void updateStatistics(
        int retries,
        int workersStarted,
        List<Long> operationDurations,
        List<OperationBoundTime> boundTimes
    ) {
        collector.recordStatisticsUpdate(nodeId, retries, workersStarted, operationDurations, boundTimes);
    }
}

