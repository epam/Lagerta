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

package org.apache.ignite.activestore.impl.publisher;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.ClusterMode;
import org.apache.ignite.activestore.commons.EndpointUtils;
import org.apache.ignite.activestore.commons.Reference;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.config.*;
import org.apache.ignite.activestore.impl.util.AtomicsHelper;
import org.jetbrains.annotations.NotNull;
import org.apache.ignite.activestore.impl.util.AtomicsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.xml.ws.Endpoint;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.apache.ignite.activestore.commons.UUIDFormat.f;
import static org.apache.ignite.activestore.impl.util.AtomicsHelper.getReference;

/**
 * @author Andrei_Yakushin
 * @since 12/5/2016 3:46 PM
 */
public class Commander implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Commander.class);

    public static final String MAIN_ID_ATOMIC_NAME = "mainIdReference";
    public static final String MODE_ATOMIC_NAME = "modeReference";

    private final Ignite ignite;
    private final RPCManager manager;
    private final UUID clusterId;
    private final String address;
    private final String superClusterAddress;
    private final DataRecoveryConfig dataRecoveryConfig;
    private final Provider<ActiveCacheStoreService> mainInterfaceProvider;
    private final PublisherReplicaService replicaService;

    private Reference<Map<UUID, ReplicaMetadata>> replicasMetadatas;
    private Reference<UUID> main;
    private Endpoint endpoint;

    @Inject
    public Commander(
        Ignite ignite,
        RPCManager manager,
        @Named(DataCapturerBusConfiguration.CLUSTER_ID) UUID clusterId,
        @Named(DataCapturerBusConfiguration.RPC_ADDRESS) String address,
        @Named(DataCapturerBusConfiguration.SUPER_CLUSTER_ADDRESS) String superClusterAddress,
        DataRecoveryConfig dataRecoveryConfig,
        Provider<ActiveCacheStoreService> mainInterfaceProvider,
        PublisherReplicaService replicaService
    ) {
        this.ignite = ignite;
        this.manager = manager;
        this.clusterId = clusterId;
        this.address = address;
        this.superClusterAddress = superClusterAddress;
        this.dataRecoveryConfig = dataRecoveryConfig;
        this.mainInterfaceProvider = mainInterfaceProvider;
        this.replicaService = replicaService;
    }

    @Override public void close() {
        if (endpoint != null) {
            endpoint.stop();
        }
    }

    public void init() {
        ignite.addCacheConfiguration(AtomicsHelper.getConfig());
        replicasMetadatas = replicaService.getReplicaMetadatasRef(true);
        replicasMetadatas.initIfAbsent(new HashMap<UUID, ReplicaMetadata>());
        main = getReference(ignite, MAIN_ID_ATOMIC_NAME, true);
        main.set(null);

        endpoint = EndpointUtils.publish(address, ActiveCacheStoreService.NAME, mainInterfaceProvider.get());

        String mainSocket = getRpcService(superClusterAddress)
            .get(SuperCluster.class, SuperCluster.NAME)
            .getMain(address);
        boolean isMain = address.equals(mainSocket);
        AtomicsHelper.<ClusterMode>getReference(ignite, MODE_ATOMIC_NAME, true)
            .set(isMain ? ClusterMode.MAIN : ClusterMode.DR);

        if (isMain) {
            processReplicaRegistration(clusterId, dataRecoveryConfig.getReplicaConfig(address), true);
        } else {
            getRpcService(mainSocket)
                .get(ActiveCacheStoreService.class, ActiveCacheStoreService.NAME)
                .register(clusterId, dataRecoveryConfig.getReplicaConfig(address));
        }
    }

    @NotNull
    private RPCService getRpcService(String address) {
        return new RPCServiceImpl(address);
    }

    public void processReplicaRegistration(UUID replicaId, ReplicaConfig replicaConfig, boolean isMain) {
        LOGGER.info("[M] Registering replica for cluster {} in {}", f(replicaId), f(clusterId));
        Map<UUID, ReplicaMetadata> map = replicasMetadatas.get();

        map.put(replicaId, new ReplicaMetadata(replicaConfig, false));
        replicasMetadatas.set(map);
        if (isMain) {
            main.set(replicaId);
        }
        ignite.compute().broadcast(new PublisherUpdater());

        UUID[] ids = map.keySet().toArray(new UUID[map.size()]);
        ReplicaConfig[] configs = new ReplicaConfig[map.size()];
        Iterator<ReplicaMetadata> metadataIt = map.values().iterator();
        for (int i = 0; i < map.size(); i++) {
            configs[i] = metadataIt.next().config();
        }
        for (RPCService service : manager.all()) {
            service.get(ActiveCacheStoreService.class, ActiveCacheStoreService.NAME).registerAll(ids, configs, clusterId);
        }
    }

    public void processAllReplicasRegistration(UUID[] ids, ReplicaConfig[] configs, UUID mainClusterId) {
        LOGGER.info("[G] Registering replica for clusters {} in {}", ids, f(clusterId));
        Map<UUID, ReplicaMetadata> map = new HashMap<>(ids.length);
        for (int i = 0; i < ids.length; i++) {
            map.put(ids[i], new ReplicaMetadata(configs[i], false));
        }
        replicasMetadatas.set(map);
        main.set(mainClusterId);
        ignite.compute().broadcast(new PublisherUpdater());
    }

    public void processNodeUnsubscribedFromReplicaNotification(UUID replicaId) {
        Map<UUID, ReplicaMetadata> map = replicasMetadatas.get();
        ReplicaMetadata metadata = map.get(replicaId);

        if (metadata.isOutOfOrder()) {
            return;
        }
        map.put(replicaId, new ReplicaMetadata(metadata.config(), true));
        replicasMetadatas.set(map);
    }
}
