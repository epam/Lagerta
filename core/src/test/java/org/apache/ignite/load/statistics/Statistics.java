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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Common access point to enable performance statistics collection and aggregation.
 */
public class Statistics {
    public final AtomicInteger retries = new AtomicInteger();
    public final AtomicInteger workers = new AtomicInteger();
    public final Queue<Long> operationDurations = new ConcurrentLinkedQueue<>();
    public final Queue<OperationBoundTime> operationBoundTimes = new ConcurrentLinkedQueue<>();

    public void recordRetry() {
        retries.incrementAndGet();
    }

    public void recordWorkersThreadStarted(int startedWorkers) {
        workers.addAndGet(startedWorkers);
    }

    /**
     * Record duration of an operation.
     *
     * @param duration
     *     Duration of some operation under tests in milliseconds.
     */
    public void recordOperation(long duration) {
        operationDurations.add(duration);
    }

    public void recordOperationStartTime(long operationId, long startTime) {
        operationBoundTimes.add(new OperationBoundTime(operationId, startTime, OperationBoundType.START));
    }

    public void recordOperationEndTime(long operationId, long endTime) {
        operationBoundTimes.add(new OperationBoundTime(operationId, endTime, OperationBoundType.END));
    }

    public boolean isEmpty() {
        return retries.get() <= 0 && workers.get() <= 0 && operationDurations.isEmpty()
            && operationBoundTimes.isEmpty();
    }
}
