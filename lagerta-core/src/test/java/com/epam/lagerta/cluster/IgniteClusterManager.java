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

import org.apache.ignite.Ignite;

import java.util.List;

/**
 * Exposes control over starting and stopping cluster for tests.
 */
public interface IgniteClusterManager {
    /**
     * Starts ignite cluster.
     *
     * @param clusterSize number of nodes in cluster.
     * @return one of grid instances which may be referred for submitting tasks.
     */
    Ignite startCluster(int clusterSize);

    /**
     * Stops all previously created nodes.
     */
    void stopCluster();

    List<Ignite> nodes();
}
