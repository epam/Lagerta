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
import org.apache.ignite.services.ServiceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

public class IgniteStopper {
    private static final Logger LOG = LoggerFactory.getLogger(IgniteStopper.class);
    private final Ignite localIgnite;

    public IgniteStopper(Ignite ignite) {
        this.localIgnite = ignite;
    }

    public void stopAllServerNodes() {
        localIgnite.compute(localIgnite.cluster().forServers().forRemotes()).withAsync()
                .broadcast(new ServerNodeStopper());
    }

    public UUID stopServerNodesWithService(String serviceName) {
        UUID nodeWithService = getNodeIDForService(serviceName);
        localIgnite.compute(localIgnite.cluster().forNodeId(nodeWithService)).withAsync()
                .run(new ServerNodeStopper());
        return nodeWithService;
    }

    public UUID getNodeIDForService(String serviceName) {
        ServiceDescriptor descriptor = localIgnite.services().serviceDescriptors().stream()
                .filter(serviceDescriptor -> serviceDescriptor.name().equalsIgnoreCase(serviceName))
                .findFirst().orElseThrow(() -> new RuntimeException("Service was not deployed"));
        Map<UUID, Integer> uuidIntegerMap = descriptor.topologySnapshot();
        if (uuidIntegerMap.isEmpty()) {
            throw new RuntimeException("Service was not deployed");
        }

        return uuidIntegerMap.keySet().iterator().next();
    }

    private static class ServerNodeStopper implements IgniteRunnable {
        @IgniteInstanceResource
        private transient Ignite ignite;

        @Override
        public void run() {
            LOG.debug("Stop node: {}", ignite.cluster().localNode());
            System.exit(1);
        }
    }
}
