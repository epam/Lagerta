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

import java.util.UUID;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Andrei_Yakushin
 * @since 1/17/2017 11:24 AM
 */
class UnsubscriberOnFailWrapperImpl implements UnsubscriberOnFailWrapper {
    private final ReplicaProducersUpdater updater;
    private final UUID clusterId;

    public UnsubscriberOnFailWrapperImpl(ReplicaProducersUpdater updater, UUID clusterId) {
        this.updater = updater;
        this.clusterId = clusterId;
    }

    @Override
    public Future<RecordMetadata> wrap(Future<RecordMetadata> future) {
        return new FutureUnsubscribeOnFail(updater, clusterId, future);
    }

    @Override
    public Future<RecordMetadata> empty(Exception e) {
        updater.unsubscribe(clusterId, e);
        return EmptyFuture.EMPTY;
    }
}
