/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lagerta.kafka;

import com.epam.lagerta.capturer.SuspendableProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class SuspendingFuture implements Future<RecordMetadata> {
    private final Future<RecordMetadata> wrapped;
    private final SuspendableProducer producer;

    public SuspendingFuture(Future<RecordMetadata> wrapped, SuspendableProducer producer) {
        this.wrapped = wrapped;
        this.producer = producer;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return wrapped.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return wrapped.isCancelled();
    }

    @Override
    public boolean isDone() {
        return wrapped.isDone();
    }

    @Override
    public RecordMetadata get() {
        try {
            return wrapped.get();
        } catch (InterruptedException | ExecutionException e) {
            producer.suspend(e);
            return null;
        }
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) {
        try {
            return wrapped.get(timeout, unit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            producer.suspend(e);
            return null;
        }
    }
}
