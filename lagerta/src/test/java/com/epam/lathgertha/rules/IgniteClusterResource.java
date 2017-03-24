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
package com.epam.lathgertha.rules;

import com.epam.lathgertha.cluster.IgniteClusterManager;
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

/**
 * Core class for tests which performs set up and tear down of cluster and exposes some useful methods.
 */
public class IgniteClusterResource {
    private static final long CLEANUP_AWAIT_TIME = 1_000;
    /**
     * Root node which is used for submitting tasks.
     */
    private Ignite root;

    private final int numberOfNodes;

    private List<CacheConfiguration> cacheConfigs;
    private ServiceConfiguration[] serviceConfigs;

    /**
     * Cluster manager to use in tests.
     */
    private IgniteClusterManager clusterManager;

    public IgniteClusterResource(int numberOfNodes, IgniteClusterManager clusterManager) {
        this.numberOfNodes = numberOfNodes;
        this.clusterManager = clusterManager;
    }

    /**
     * Returns root grid node.
     */
    public Ignite ignite() {
        return root;
    }

    public List<Ignite> nodes() {
        return clusterManager.nodes();
    }

    public Ignite startCluster() {
        root = clusterManager.startCluster(numberOfNodes);
        cacheConfigs = getNonSystemCacheConfigs();
        serviceConfigs = root.configuration().getServiceConfiguration();
        return root;
    }

    public void stopCluster() {
        root.services().cancelAll();
        clusterManager.stopCluster();
    }

    private List<CacheConfiguration> getNonSystemCacheConfigs() {
        return Arrays
            .stream(root.configuration().getCacheConfiguration())
            .filter(config -> !GridCacheUtils.isSystemCache(config.getName()))
            .collect(Collectors.toList());
    }

    public void clearCluster() {
        cacheConfigs.forEach(config -> root.destroyCache(config.getName()));
        root.createCaches(cacheConfigs);
        if (serviceConfigs != null) {
            IgniteServices services = root.services();

            services.cancelAll();
            Arrays.stream(serviceConfigs).forEach(services::deploy);
        }
        Uninterruptibles.sleepUninterruptibly(CLEANUP_AWAIT_TIME, TimeUnit.MILLISECONDS);
    }


}
