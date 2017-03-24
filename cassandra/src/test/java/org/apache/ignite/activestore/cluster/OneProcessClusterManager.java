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
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

import static org.apache.ignite.activestore.cluster.IgniteConfigHelper.setNumberProcesses;

/**
 * @author Aleksandr_Meterko
 * @since 9/27/2016
 */
public class OneProcessClusterManager extends DefaultOneProcessClusterManager {
    /** {@inheritDoc} */
    @Override protected Ignite startGrid(IgniteConfiguration igniteConfiguration, int gridNumber) {
        IgniteConfiguration configuration = new IgniteConfiguration(igniteConfiguration);
        configuration.setGridName("node-" + gridNumber);
        setNumberProcesses(configuration, 1);
        return Ignition.start(configuration);
    }
}
