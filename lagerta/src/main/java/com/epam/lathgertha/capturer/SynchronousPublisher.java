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
package com.epam.lathgertha.capturer;

import org.apache.kafka.clients.producer.RecordMetadata;

import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SynchronousPublisher implements ModificationListener {
    private final Supplier<List<TransactionalProducer>> producers;

    public SynchronousPublisher(Supplier<List<TransactionalProducer>> producers) {
        this.producers = producers;
    }

    @Override
    public void handle(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates)
            throws CacheWriterException {
        List<Future<RecordMetadata>> futures = producers
                .get()
                .stream()
                .map(producer -> producer.send(transactionId, updates))
                .collect(Collectors.toList());
        try {
            for (Future<RecordMetadata> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new CacheWriterException(e);
        }
    }
}
