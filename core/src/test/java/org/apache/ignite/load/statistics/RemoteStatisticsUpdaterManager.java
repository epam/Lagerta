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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import org.apache.ignite.IgniteCompute;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/11/2017 4:46 PM
 */
class RemoteStatisticsUpdaterManager {
    private final Statistics stats;
    private final StatisticsConfig config;
    private final ExecutorManager executorManager;

    private volatile boolean started = false;

    @Inject
    public RemoteStatisticsUpdaterManager(
        Statistics stats,
        StatisticsConfig config,
        ExecutorManager executorManager
    ) {
        this.stats = stats;
        this.config = config;
        this.executorManager = executorManager;
    }

    public boolean isStarted() {
        return started;
    }

    public void start(IgniteCompute aggregatorNodeCompute, UUID localNodeId) {
        started = true;
        long reportFrequency = config.getReportFrequency();
        Runnable task = new RemoteStatisticsUpdater(
            localNodeId,
            stats,
            aggregatorNodeCompute
        );
        executorManager.getExecutor()
            .scheduleAtFixedRate(task, reportFrequency, reportFrequency, TimeUnit.MILLISECONDS);
    }
}
