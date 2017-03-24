

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

package org.apache.ignite.activestore.load.statistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.activestore.load.TestsHelper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.log4j.Logger;

/**
 * Common access point to enable performance statistics collection and aggregation.
 */
public final class Statistics {
    /** */
    private static final Logger LOG = Logger.getLogger(Statistics.class);

    /** */
    private static final ConcurrentLinkedQueue<Long> LOCAL_OPERATION_DURATIONS = new ConcurrentLinkedQueue<>();

    /** */
    private static final AtomicInteger RETRIES = new AtomicInteger();

    /** */
    private static final AtomicInteger WORKERS = new AtomicInteger();

    /** */
    private static boolean reporterStarted = false;

    /** */
    private static StatisticsCollector collector;

    /**
     * Turns on statistics collection and possibly aggregation across already started ignite cluster.
     *
     * @param ignite
     *     Ignite client.
     */
    public static void enable(Ignite ignite, final StatisticsCollector statisticsCollector) {
        collector = statisticsCollector;
        StatisticsDeploymentHelper.deployContinuously(ignite);
    }

    /** */
    private static void reportServerPerformance(UUID serverNodeId, List<Long> operationsDurations, int retries,
        int workersStarted) {
        LOG.info("Got performance report from node " + serverNodeId);
        if (retries > 0) {
            collector.recordOperationsRetries(retries);
        }
        if (operationsDurations.size() > 0) {
            collector.recordOperationsBatch(operationsDurations.size());
        }
        if (workersStarted > 0) {
            collector.recordWorkersThreadStarted(workersStarted);
        }
        for (long duration : operationsDurations) {
            collector.recordOperationLatency(duration);
        }
    }

    /** */
    private static class StatisticsDeploymentHelper {
        /** */
        private final Ignite ignite;

        /** */
        private final UUID aggregatorNodeId;

        /** */
        private Long lastTopologyVersion;

        /** */
        private Set<UUID> lastTopology = new HashSet<>();

        /** */
        StatisticsDeploymentHelper(Ignite ignite) {
            this.ignite = ignite;
            aggregatorNodeId = ignite.cluster().localNode().id();
        }

        /** */
        private void deployStatisticsCollector() {
            long currentTopologyVersion = ignite.cluster().topologyVersion();
            Set<UUID> currentTopology = new HashSet<>();
            Set<UUID> newNodes = new HashSet<>();

            if ((lastTopologyVersion == null) || (currentTopologyVersion != lastTopologyVersion)) {
                for (ClusterNode node : ignite.cluster().topology(currentTopologyVersion)) {
                    currentTopology.add(node.id());
                    if ((aggregatorNodeId != node.id()) && !lastTopology.contains(node.id())) {
                        newNodes.add(node.id());
                    }
                }
                lastTopologyVersion = currentTopologyVersion;
                lastTopology = currentTopology;
            } else {
                return;
            }
            Collection<UUID> response = ignite.compute(ignite.cluster().forNodeIds(newNodes)).broadcast(
                new IgniteCallable<UUID>() {
                    @Override public UUID call() {
                        if (!reporterStarted) {
                            IgniteCompute compute = ignite.compute(ignite.cluster().forNodeId(aggregatorNodeId));
                            UUID localNodeId = ignite.cluster().localNode().id();

                            new RemoteNodeReporter(compute, localNodeId).start();
                            reporterStarted = true;
                            return localNodeId;
                        }
                        return null;
                    }
                });
            for (UUID nodeId : response) {
                if (nodeId != null) {
                    LOG.info("Started statistics collector on node " + nodeId);
                }
            }
        }

        static void deployContinuously(Ignite ignite) {
            long reportFrequency = TestsHelper.getLoadTestsStatisticsReportFrequency();
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            final StatisticsDeploymentHelper helper = new StatisticsDeploymentHelper(ignite);

            executor.scheduleAtFixedRate(new Runnable() {
                @Override public void run() {
                    helper.deployStatisticsCollector();
                }
            }, 0, reportFrequency, TimeUnit.MILLISECONDS);
        }
    }

    /** */
    private static class RemoteNodeReporter {
        /** */
        private final IgniteCompute aggregatorNodeCompute;

        /** */
        private final UUID localNodeId;

        /** */
        private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        /** */
        RemoteNodeReporter(IgniteCompute aggregatorNodeCompute, UUID localNodeId) {
            this.aggregatorNodeCompute = aggregatorNodeCompute;
            this.localNodeId = localNodeId;
        }

        /** */
        public void start() {
            long reportFrequency = TestsHelper.getLoadTestsStatisticsReportFrequency();

            executor.scheduleAtFixedRate(new Runnable() {
                @Override public void run() {
                    int reportSize = LOCAL_OPERATION_DURATIONS.size();

                    if (reportSize > 0 || RETRIES.get() > 0 || WORKERS.get() > 0) {
                        final List<Long> currentDurations = new ArrayList<>(reportSize);
                        final int currentRetries = RETRIES.getAndSet(0);
                        final int workersStarted = WORKERS.getAndSet(0);

                        for (int i = 0; i < reportSize; i++) {
                            currentDurations.add(LOCAL_OPERATION_DURATIONS.poll());
                        }
                        aggregatorNodeCompute.run(new IgniteRunnable() {
                            @Override public void run() {
                                reportServerPerformance(localNodeId, currentDurations, currentRetries, workersStarted);
                            }
                        });
                    }
                }
            }, reportFrequency, reportFrequency, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Record duration of an operation.
     *
     * @param duration
     *     Duration of some operation under tests in milliseconds.
     */
    public static void recordOperation(long duration) {
        LOCAL_OPERATION_DURATIONS.add(duration);
    }

    /** */
    public static void recordRetry() {
        RETRIES.incrementAndGet();
    }

    /** */
    public static void recordWorkersThreadStarted(int startedWorkers) {
        WORKERS.addAndGet(startedWorkers);
    }
}
