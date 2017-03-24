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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.Reference;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;
import org.apache.ignite.activestore.impl.config.ReplicaMetadata;
import org.apache.ignite.activestore.impl.util.AtomicsHelper;

/**
 * @author Evgeniy_Ignatiev
 * @since 14:58 12/09/2016
 */
@Singleton
class PublisherReplicaService {
    private static final String PREFIX = "reconciliation_";
    private static final String REPLICAS_CONFIGS_ATOMIC_NAME = "replicasConfigsReference";

    private final Ignite ignite;

    @Inject
    public PublisherReplicaService(Ignite ignite) {
        this.ignite = ignite;
    }

    public Map<UUID, ReplicaMetadata> getReplicaMetadatas() {
        return getReplicaMetadatasRef(false).get();
    }

    public Map<UUID, ReplicaConfig> getReplicaConfigs() {
        Map<UUID, ReplicaMetadata> replicaMetadatas = getReplicaMetadatas();
        Map<UUID, ReplicaConfig> replicaConfigs = new HashMap<>(replicaMetadatas.size());

        for (Map.Entry<UUID, ReplicaMetadata> entry : replicaMetadatas.entrySet()) {
            replicaConfigs.put(entry.getKey(), entry.getValue().config());
        }
        return replicaConfigs;
    }

    public Reference<Map<UUID, ReplicaMetadata>> getReplicaMetadatasRef(boolean create) {
        return AtomicsHelper.getReference(ignite, REPLICAS_CONFIGS_ATOMIC_NAME, create);
    }

    public ReplicaConfig getReplicaConfig(UUID replicaId) {
        return getReplicaMetadatas().get(replicaId).config();
    }

    public String toGroupId(UUID replicaId) {
        return PREFIX + replicaId;
    }
}
