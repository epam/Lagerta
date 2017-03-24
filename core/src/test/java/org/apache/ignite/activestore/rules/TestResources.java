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

package org.apache.ignite.activestore.rules;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.cluster.IgniteClusterManager;
import org.junit.Assert;

/**
 * Core class for tests which performs set up and tear down of cluster and exposes some useful methods.
 */
public class TestResources extends MeteredResource {

    /**
     * Root node which is used for submitting tasks.
     */
    private Ignite root;

    private final int numberOfNodes;

    /**
     * Cluster manager to use in tests.
     */
    private IgniteClusterManager clusterManager;

    public TestResources(String resourceName, int numberOfNodes) {
        super(resourceName);
        this.numberOfNodes = numberOfNodes;
    }

    /**
     * Set cluster manager.
     */
    public void setClusterManager(IgniteClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    /**
     * Returns root grid node.
     */
    public Ignite ignite() {
        return root;
    }

    public IgniteClusterCleanupRule perTestMethodCleanupRule() {
        return new IgniteClusterCleanupRule(root);
    }

    /** {@inheritDoc} */
    @Override protected void setUp() {
        Assert.assertNotNull("Cluster manager must be set in resources", clusterManager);
        root = clusterManager.startCluster(numberOfNodes);
    }

    /** {@inheritDoc} */
    @Override protected void tearDown() {
        ignite().services().cancelAll();
        clusterManager.stopCluster();
    }

}
