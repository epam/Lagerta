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

import com.codahale.metrics.MetricRegistry;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects and reports statistics using metrics library.
 */
public class StatisticsCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsCollector.class);

    public static final String THROUGHPUT_METER = "throughput";
    public static final String RETRY_COUNTER = "retries";
    public static final String GANGLIA_METRICS_PREFIX = "load_test";
    public static final String LATENCY_HISTOGRAM = "latency";
    public static final String LOAD_THREADS_COUNTER = "load-threads";

    private final MetricRegistry registry;

    private final Map<Long, Long> operationBoundTimes = new ConcurrentHashMap<>();

    @Inject
    public StatisticsCollector(MetricRegistry registry) {
        this.registry = registry;
    }

    /**
     * Updates metrics with the latency of the recently completed operation.
     *
     * @param duration Latency of the recent operation in milliseconds.
     */
    public void recordOperationLatency(long duration) {
        registry.histogram(LATENCY_HISTOGRAM).update(duration);
    }

    public void recordOperationsBatch(long batchSize) {
        registry.meter(THROUGHPUT_METER).mark(batchSize);
    }

    public void recordPartialOperations(List<OperationBoundTime> boundTimes) {
        int finishedOperations = 0;

        for (OperationBoundTime operationBoundTime : boundTimes) {
            long operationId = operationBoundTime.getOperationId();

            if (operationBoundTimes.containsKey(operationId)) {
                long duration =  operationBoundTime.getTime() - operationBoundTimes.remove(operationId);

                if (operationBoundTime.getBoundType() == OperationBoundType.START) {
                    duration *= -1;
                }
                recordOperationLatency(duration);
                finishedOperations++;
            }
            else {
                operationBoundTimes.put(operationId, operationBoundTime.getTime());
            }
        }
        recordOperationsBatch(finishedOperations);
    }

    public void recordOperationsRetries(int retries) {
        registry.counter(RETRY_COUNTER).inc(retries);
    }

    public void recordWorkersThreadStarted(int workers) {
        registry.counter(LOAD_THREADS_COUNTER).inc(workers);
    }

    public void recordStatisticsUpdate(
        UUID nodeId,
        int retries,
        int workersStarted,
        @Nullable List<Long> operationDurations,
        @Nullable List<OperationBoundTime> operationBoundTimes
    ) {
        LOGGER.info("[T] Got statistics report from node {}", nodeId);
        if (retries > 0) {
            recordOperationsRetries(retries);
        }
        if (workersStarted > 0) {
            recordWorkersThreadStarted(workersStarted);
        }
        if (operationDurations != null && !operationDurations.isEmpty()) {
            recordOperationsBatch(operationDurations.size());
            for (long duration : operationDurations) {
                recordOperationLatency(duration);
            }
        }
        if (operationBoundTimes != null && !operationBoundTimes.isEmpty()) {
            recordPartialOperations(operationBoundTimes);
        }
    }
}
