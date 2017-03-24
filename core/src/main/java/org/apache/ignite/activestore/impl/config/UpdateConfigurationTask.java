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

import java.util.Map;
import java.util.UUID;

/**
 * @author Evgeniy_Ignatiev
 * @since 2/1/2017 7:52 PM
 */
class UpdateConfigurationTask implements Runnable {
    private final ReplicaProducersManagerUpdater producersManagerUpdater;
    private final Map<UUID, ReplicaConfig> replicasConfigs;

    public UpdateConfigurationTask(
        ReplicaProducersManagerUpdater producersManagerUpdater,
        Map<UUID, ReplicaConfig> replicasConfigs
    ) {
        this.producersManagerUpdater = producersManagerUpdater;
        this.replicasConfigs = replicasConfigs;
    }

    @Override public void run() {
        producersManagerUpdater.updateConfiguration(replicasConfigs);
    }
}
