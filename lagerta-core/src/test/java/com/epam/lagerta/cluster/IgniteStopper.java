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
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteStopper {
    private static final Logger LOG = LoggerFactory.getLogger(IgniteStopper.class);
    private final Ignite localIgnite;

    public IgniteStopper(Ignite ignite) {
        this.localIgnite = ignite;
    }

    public void stopAllServerNodes() {
        localIgnite.compute(localIgnite.cluster().forServers()).withAsync().
                broadcast(new StopServerNode());
    }

    public void stopServerNodesWithService(String serviceName) {
        localIgnite.compute(localIgnite.cluster().forServers()).
                broadcast(new StopServerNodeWithService(serviceName));
    }

    private static class StopServerNode implements IgniteRunnable {
        @IgniteInstanceResource
        private transient Ignite ignite;

        @Override
        public void run() {
            LOG.debug("Stop node: {}", ignite.cluster().localNode());
            System.exit(1);
        }
    }

    private static class StopServerNodeWithService implements IgniteRunnable {
        @IgniteInstanceResource
        private transient Ignite ignite;
        private final String serviceName;

        public StopServerNodeWithService(String serviceName) {
            this.serviceName = serviceName;
        }

        @Override
        public void run() {
            Object service = ignite.services().service(serviceName);
            if (service != null) {
                LOG.debug("Stop node: {} \nWith service: {}", ignite.cluster().localNode(), serviceName);
                System.exit(1);
            }
        }
    }
}
