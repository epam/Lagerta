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
package com.epam.lagerta.resources;

import com.epam.lagerta.cluster.IgniteClusterManager;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.ignite.Ignite;

import java.util.concurrent.TimeUnit;

/**
 * Core class for tests which performs set up and tear down of cluster and exposes some useful methods.
 */
public class IgniteClusterResource implements Resource {
    /**
     * Root node which is used for submitting tasks.
     */
    private Ignite root;

    private final int numberOfNodes;

    /**
     * Cluster manager to use in tests.
     */
    private IgniteClusterManager clusterManager;

    public IgniteClusterResource(int numberOfNodes, IgniteClusterManager clusterManager) {
        this.numberOfNodes = numberOfNodes;
        this.clusterManager = clusterManager;
    }

    public IgniteClusterResource(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }

    public void setClusterManager(IgniteClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    /**
     * Returns root grid node.
     */
    public Ignite ignite() {
        return root;
    }

    @Override
    public void setUp() {
        root = clusterManager.startCluster(numberOfNodes);
        Uninterruptibles.sleepUninterruptibly(IgniteClusterManager.AWAIT_TIME, TimeUnit.MILLISECONDS);
    }

    @Override
    public void tearDown() {
        clusterManager.stopCluster();
    }

    public void clearCluster() {
        clusterManager.reloadCluster();
    }
}
