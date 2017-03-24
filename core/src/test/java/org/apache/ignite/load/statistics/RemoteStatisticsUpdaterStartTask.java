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

package org.apache.ignite.load.statistics;

import java.util.UUID;

import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/11/2017 4:48 PM
 */
class RemoteStatisticsUpdaterStartTask extends ActiveStoreIgniteCallable<UUID> {
    private final UUID aggregatorNodeId;

    @IgniteInstanceResource
    private transient Ignite ignite;

    @Inject
    private transient RemoteStatisticsUpdaterManager reporter;

    public RemoteStatisticsUpdaterStartTask(UUID aggregatorNodeId) {
        this.aggregatorNodeId = aggregatorNodeId;
    }

    @Override public UUID callInjected() {
        if (!reporter.isStarted()) {
            IgniteCompute compute = ignite.compute(ignite.cluster().forNodeId(aggregatorNodeId));
            UUID localNodeId = ignite.cluster().localNode().id();

            reporter.start(compute, localNodeId);
            return localNodeId;
        }
        return null;
    }
}
