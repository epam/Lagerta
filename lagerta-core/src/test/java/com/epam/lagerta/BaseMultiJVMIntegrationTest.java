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

package com.epam.lagerta;

import com.epam.lagerta.cluster.DifferentJVMClusterManager;
import com.epam.lagerta.resources.FullClusterResource;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.BeforeSuite;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public abstract class BaseMultiJVMIntegrationTest extends BaseIntegrationTest {

    private static final DifferentJVMClusterManager JVM_CLUSTER_MANAGER = new DifferentJVMClusterManager();
    private static final int WAIT_SHUTDOWN = 100;

    @BeforeSuite
    public void setUp() throws Exception {
        ALL_RESOURCES.setClusterManager(JVM_CLUSTER_MANAGER);
        ALL_RESOURCES.setUp();
    }

    public UUID stopNodeWithService(String serviceName) {
        return JVM_CLUSTER_MANAGER.getIgniteStopper().stopServerNodesWithService(serviceName);
    }

    public UUID getNodeIdForService(String serviceName) {
        return JVM_CLUSTER_MANAGER.getIgniteStopper().getNodeIDForService(serviceName);
    }

    public void waitShutdownOneNode() {
        while (FullClusterResource.CLUSTER_SIZE - 1 != JVM_CLUSTER_MANAGER.getCountAliveServerNodes()) {
            Uninterruptibles.sleepUninterruptibly(WAIT_SHUTDOWN, TimeUnit.MILLISECONDS);
        }
    }

    public void awaitStartAllServerNodes() {
        JVM_CLUSTER_MANAGER.startServerNodes(FullClusterResource.CLUSTER_SIZE);
        JVM_CLUSTER_MANAGER.waitStartServerNodes();
        awaitTransactions();
    }
}
