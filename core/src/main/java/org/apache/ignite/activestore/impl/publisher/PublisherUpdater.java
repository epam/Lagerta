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

import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteRunnable;
import org.apache.ignite.activestore.impl.config.RPCUpdater;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;
import org.apache.ignite.activestore.impl.config.ReplicaProducersUpdater;
import org.apache.ignite.activestore.impl.util.AtomicsHelper;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * @author Evgeniy_Ignatiev
 * @since 2/1/2017 4:56 PM
 */
class PublisherUpdater extends ActiveStoreIgniteRunnable {
    @IgniteInstanceResource
    private transient Ignite ignite;

    @Inject
    private transient ReplicaProducersUpdater replicaProducersManager;

    @Inject
    private transient RPCUpdater rpcUpdater;

    @Inject
    private transient PublisherReplicaService replicaService;

    @Override protected void runInjected() {
        Map<UUID, ReplicaConfig> replicasConfigs = replicaService.getReplicaConfigs();

        replicaProducersManager.updateConfiguration(replicasConfigs);
        rpcUpdater.updateConfiguration(
            replicasConfigs,
            AtomicsHelper.<UUID>getReference(ignite, Commander.MAIN_ID_ATOMIC_NAME, false).get()
        );
    }
}
