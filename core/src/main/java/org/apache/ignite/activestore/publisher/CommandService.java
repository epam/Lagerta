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

package org.apache.ignite.activestore.publisher;

import java.util.UUID;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;
import org.apache.ignite.services.Service;

/**
 * @author Andrei_Yakushin
 * @since 12/5/2016 3:43 PM
 */
public interface CommandService extends Service {
    String SERVICE_NAME = "commandService";

    /** */
    void register(UUID replicaId, ReplicaConfig config, boolean isMain);

    /** */
    void registerAll(UUID[] ids, ReplicaConfig[] configs, UUID mainClusterId);

    void notifyNodeUnsubscribedFromReplica(UUID replicaId);
}
