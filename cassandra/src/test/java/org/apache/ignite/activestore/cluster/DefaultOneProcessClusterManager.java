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

package org.apache.ignite.activestore.cluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Implementation of {@link IgniteClusterManager} which starts all grids inside one process and uses Java configuration.
 * Abstract manager which starts all grids in one process.
 */
public abstract class DefaultOneProcessClusterManager implements IgniteClusterManager {
    /**
     * Started grids.
     */
    private Ignite[] servers;

    /** {@inheritDoc} */
    @Override public Ignite startCluster(int nodes, IgniteConfiguration igniteConfiguration) {
        servers = new Ignite[nodes];
        for (int i = 0; i < nodes; i++) {
            servers[i] = startGrid(igniteConfiguration, i);
        }
        return servers[0];
    }

    /*
    * Starts single grid.
    * @param igniteConfiguration base configuration.
    * @param gridNumber number of started grid.
    * @return grid.
    */
    protected abstract Ignite startGrid(IgniteConfiguration igniteConfiguration, int gridNumber);

    /** {@inheritDoc} */
    @Override public void stopCluster() {
        for (Ignite server : servers) {
            server.close();
        }
    }
}
