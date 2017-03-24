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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Andrei_Yakushin
 * @since 1/17/2017 11:24 AM
 */
class FutureUnsubscribeOnFail implements Future<RecordMetadata> {
    private final ReplicaProducersUpdater updater;
    private final UUID clusterId;
    private final Future<RecordMetadata> inner;

    public FutureUnsubscribeOnFail(ReplicaProducersUpdater updater, UUID clusterId, Future<RecordMetadata> inner) {
        this.updater = updater;
        this.clusterId = clusterId;
        this.inner = inner;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return inner.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return inner.isCancelled();
    }

    @Override
    public boolean isDone() {
        return inner.isDone();
    }

    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        try {
            return inner.get();
        } catch (Exception e) {
            updater.unsubscribe(clusterId, e);
            return null;
        }
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            return inner.get(timeout, unit);
        } catch (Exception e) {
            updater.unsubscribe(clusterId, e);
            return null;
        }
    }
}
