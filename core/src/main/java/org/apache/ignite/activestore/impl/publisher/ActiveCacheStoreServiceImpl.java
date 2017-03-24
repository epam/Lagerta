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

import java.util.UUID;

import javax.inject.Inject;
import javax.jws.WebService;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;

/**
 * @author Andrei_Yakushin
 * @since 12/6/2016 11:18 AM
 */
@WebService(endpointInterface = "org.apache.ignite.activestore.impl.publisher.ActiveCacheStoreService")
public class ActiveCacheStoreServiceImpl implements ActiveCacheStoreService {
    private final CommanderService commander;
    private final IdSequencer sequencer;

    @Inject
    public ActiveCacheStoreServiceImpl(CommanderService commander, IdSequencer sequencer) {
        this.commander = commander;
        this.sequencer = sequencer;
    }

    @Override
    public void register(UUID replicaId, ReplicaConfig replicaConfig) {
        commander.register(replicaId, replicaConfig, false);
    }

    @Override
    public void registerAll(UUID[] ids, ReplicaConfig[] configs, UUID mainClusterId) {
        commander.registerAll(ids, configs, mainClusterId);
    }

    @Override
    public int ping() {
        return 0;
    }

    @Override public void startReconciliation(UUID replicaId, long startTransactionId, long endTransactionId) {
        commander.startReconciliation(replicaId, startTransactionId);
    }

    @Override public void stopReconciliation(UUID replicaId) {
        commander.stopReconciliation(replicaId);
    }

    @Override public long lastProcessedTxId() {
        return sequencer.getLastProducedId();
    }
}
