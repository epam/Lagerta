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

import org.apache.ignite.activestore.impl.config.ReplicaConfig;

/**
 * @author Evgeniy_Ignatiev
 * @since 2/1/2017 5:06 PM
 */
class AllReplicasRegisterTask implements Runnable {
    private final Commander commander;
    private final UUID[] ids;
    private final ReplicaConfig[] configs;
    private final UUID mainClusterId;

    public AllReplicasRegisterTask(Commander commander, UUID[] ids,
        ReplicaConfig[] configs, UUID mainClusterId) {
        this.commander = commander;
        this.ids = ids;
        this.configs = configs;
        this.mainClusterId = mainClusterId;
    }

    @Override public void run() {
        commander.processAllReplicasRegistration(ids, configs, mainClusterId);
    }
}
