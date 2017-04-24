/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lagerta.cluster;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.services.ServiceConfiguration;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Implementation of {@link IgniteClusterManager} which starts all grids inside one process and uses Java configuration.
 * Abstract manager which starts all grids in one process.
 */
public abstract class DefaultOneProcessClusterManager extends BaseIgniteClusterManager {
    /**
     * Started grids.
     */
    private List<Ignite> servers;

    /** {@inheritDoc} */
    @Override
    public Ignite startCluster(int clusterSize) {
        servers = IntStream
                .range(0, clusterSize)
                .mapToObj(gridNumber -> startGrid(gridNumber, clusterSize))
                .collect(Collectors.toList());
        clientNode = servers.get(0);
        cacheConfigs = getNonSystemCacheConfigs();
        serviceConfigs = clientNode.configuration().getServiceConfiguration();
        return clientNode;
    }

    /*
    * Starts single grid.
    * @param igniteConfiguration base configuration.
    * @param gridNumber number of started grid.
    * @param clusterSize number of nodes in a cluster.
    * @return grid.
    */
    protected abstract Ignite startGrid(int gridNumber, int clusterSize);

    /** {@inheritDoc} */
    @Override
    public void stopCluster() {
        clientNode.services().cancelAll();
        for (Ignite server : servers) {
            server.executorService().shutdownNow();
            server.close();
        }
    }

    @Override
    public void reloadCluster() {
        stopServicesAndCaches();
        startServicesAndCaches();
    }
}
