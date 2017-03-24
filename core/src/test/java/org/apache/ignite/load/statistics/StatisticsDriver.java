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

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/11/2017 4:51 PM
 */
public class StatisticsDriver implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsDriver.class);

    private final Ignite ignite;
    private final StatisticsConfig config;
    private final ExecutorManager executorManager;
    private final LocalStatisticsUpdater localStatisticsUpdater;
    private final UUID aggregatorNodeId;

    private Long lastTopologyVersion;
    private Set<UUID> lastTopology = new HashSet<>();

    @Inject
    public StatisticsDriver(
        Ignite ignite,
        StatisticsConfig config,
        ExecutorManager executorManager,
        LocalStatisticsUpdater localStatisticsUpdater
    ) {
        this.ignite = ignite;
        this.config = config;
        this.executorManager = executorManager;
        this.localStatisticsUpdater = localStatisticsUpdater;
        aggregatorNodeId = ignite.cluster().localNode().id();
    }

    public void startCollecting() {
        ScheduledExecutorService executor = executorManager.getExecutor();

        executor.scheduleAtFixedRate(this, 0, config.getReportFrequency(), TimeUnit.MILLISECONDS);
        executor.scheduleAtFixedRate(
            localStatisticsUpdater,
            config.getReportFrequency(),
            config.getReportFrequency(),
            TimeUnit.MILLISECONDS
        );
    }

    @Override public void run() {
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
        }
        else {
            return;
        }
        Collection<UUID> response = ignite.compute(ignite.cluster().forNodeIds(newNodes)).broadcast(
            new RemoteStatisticsUpdaterStartTask(aggregatorNodeId)
        );
        for (UUID nodeId : response) {
            if (nodeId != null) {
                LOGGER.info("[T] Started statistics collection on node {}", nodeId);
            }
        }
    }
}
