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
package com.epam.lathgertha.kafka;

import com.epam.lathgertha.capturer.SuspendableProducer;
import com.epam.lathgertha.capturer.TransactionalProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.cache.Cache;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class SuspendableKafkaProducerImpl implements SuspendableProducer {
    private static final Future<RecordMetadata> EMPTY_FUTURE = CompletableFuture.completedFuture(null);

    private final TransactionalProducer producer;

    private volatile Exception exception;

    public SuspendableKafkaProducerImpl(TransactionalProducer producer) {
        this.producer = producer;
    }

    @Override
    public Future<RecordMetadata> send(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates) {
        if (exception != null) {
            return EMPTY_FUTURE;
        }
        try {
            Future<RecordMetadata> future = producer.send(transactionId, updates);
            return new SuspendingFuture(future, this);
        } catch (Exception e) {
            exception = e;
            return EMPTY_FUTURE;
        }
    }

    @Override
    public void resume() {
        exception = null;
    }

    @Override
    public void suspend(Exception cause) {
        exception = cause;
    }

    @Override
    public Exception getException() {
        return exception;
    }
}
