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

package org.apache.ignite.activestore.mocked.mocks;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;
import org.apache.ignite.activestore.impl.config.RPCManager;
import org.apache.ignite.activestore.impl.config.RPCService;
import org.apache.ignite.activestore.impl.config.RPCUpdater;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;
import org.jetbrains.annotations.Nullable;

/**
 * @author Aleksandr_Meterko
 * @since 1/19/2017
 */
public class MockRPCManager implements RPCManager, RPCUpdater {

    @Inject
    private MockRPCService rpcService;

    @Nullable @Override public RPCService main() {
        return rpcService;
    }

    @Override public Collection<RPCService> all() {
        return Collections.<RPCService>singletonList(rpcService);
    }

    @Override public void updateConfiguration(Map<UUID, ReplicaConfig> replicasConfigs, UUID main) {
        // no-op
    }
}
