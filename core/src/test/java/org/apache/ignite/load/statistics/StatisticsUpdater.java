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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/11/2017 4:45 PM
 */
abstract class StatisticsUpdater implements Runnable {
    protected final UUID nodeId;
    protected final Statistics stats;

    protected StatisticsUpdater(UUID nodeId, Statistics stats) {
        this.nodeId = nodeId;
        this.stats = stats;
    }

    @Override public void run() {
        if (stats.isEmpty()) {
            return;
        }
        int currentRetries = stats.retries.getAndSet(0);
        int workersStarted = stats.workers.getAndSet(0);
        List<Long> currentDurations = takeQueue(stats.operationDurations);
        List<OperationBoundTime> boundTimes = takeQueue(stats.operationBoundTimes);

        updateStatistics(currentRetries, workersStarted, currentDurations, boundTimes);
    }

    protected abstract void updateStatistics(
        int retries,
        int workersStarted,
        List<Long> operationDurations,
        List<OperationBoundTime> boundTimes
    );

    private static <V> List<V> takeQueue(Queue<V> queue) {
        int size = queue.size();

        if (size == 0) {
            return null;
        }
        List<V> result = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            result.add(queue.poll());
        }
        return result;
    }
}
