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

package org.apache.ignite.activestore.impl.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import javax.inject.Inject;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

/**
 * @author Andrei_Yakushin
 * @since 1/19/2017 3:44 PM
 */
@Singleton
public class RPCManagerImpl implements RPCManager, RPCUpdater, LifecycleAware {
    @Inject
    @Named(DataCapturerBusConfiguration.CLUSTER_ID)
    private Provider<UUID> clusterId;

    @Inject
    @Named(DataCapturerBusConfiguration.NODE_ID)
    private UUID nodeId;

    private Map<UUID, RPCService> rpc = new HashMap<>();
    private volatile UUID main = null;

    private volatile boolean active;

    @Override public void start() {
        active = true;
    }

    @Override public void stop() {
        active = false;
    }

    @Override @Nullable public RPCService main() {
        return rpc.get(main);
    }

    @Override public Collection<RPCService> all() {
        return rpc.values();
    }

    @Override public void updateConfiguration(Map<UUID, ReplicaConfig> replicasConfigs, UUID main) {
        if (!active) {
            return;
        }
        synchronized (this) {
            if (active) {
                Map<UUID, RPCService> updateRPC = new HashMap<>();
                for (Map.Entry<UUID, ReplicaConfig> entry : replicasConfigs.entrySet()) {
                    if (!entry.getKey().equals(clusterId.get())) {
                        ReplicaConfig config = entry.getValue();
                        updateRPC.put(entry.getKey(), new RPCServiceImpl(config.getAddress()));
                    }
                }
                rpc = updateRPC;
                this.main = main;
            }
        }
    }
}
