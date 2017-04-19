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

package com.epam.lagerta.cluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteStopper {
    private static final Logger LOG = LoggerFactory.getLogger(IgniteStopper.class);
    private final Ignite ignite;

    public IgniteStopper(Ignite ignite) {
        this.ignite = ignite;
    }

    public void stopAllServerNodes() {
        ignite.compute(ignite.cluster().forServers()).withAsync().
                broadcast(
                        (IgniteRunnable) () -> {
                            LOG.debug("Stop node: {}", ignite.cluster().localNode());
                            System.exit(1);
                        }
                );
    }

    public void stopServerNodesWithService(String serviceName) {
        ignite.compute(ignite.cluster().forServers()).withAsync().
                broadcast(
                        (IgniteRunnable) () -> {
                            Object service = ignite.services().service(serviceName);
                            if (service != null) {
                                LOG.debug("Stop node: {} \nWith service: {}", ignite.cluster().localNode(), serviceName);
                                System.exit(1);
                            }
                        }
                );
    }
}
