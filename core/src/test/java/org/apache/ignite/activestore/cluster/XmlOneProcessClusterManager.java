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

/**
 * Implementation of {@link IgniteClusterManager} which reads its configuration from XML file instead of Java.
 */
public class XmlOneProcessClusterManager extends DefaultOneProcessClusterManager {

    private final String configName;

    public XmlOneProcessClusterManager(String configName) {
        this.configName = configName;
    }

    /** {@inheritDoc} */
    @Override protected Ignite startGrid(int gridNumber) {
        return Ignition.start(configName);
    }
}
