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

import org.apache.ignite.activestore.KeyValueListener;
import org.apache.ignite.activestore.impl.config.ReplicaProducersManager;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Andrei_Yakushin
 * @since 12/5/2016 1:40 PM
 */
public class SynchronousPublisher implements KeyValueListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronousPublisher.class);
    @Inject
    private ReplicaProducersManager replicaProducersManager;

    @Override public void writeTransaction(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates)
        throws CacheWriterException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[M] Writing synchronous replica for transaction {} with size of batch {}",
                    transactionId, updates.size());
        }
        Collection<RemoteKafkaProducer> producers = replicaProducersManager.getProducers();

        if (!producers.isEmpty()) {
            List<Future<RecordMetadata>> futures = new ArrayList<>(producers.size());
            for (RemoteKafkaProducer producer : producers) {
                futures.add(producer.writeTransaction(transactionId, updates));
            }
            wait(futures);
        }
    }

    @Override
    public void writeGapTransaction(long transactionId) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[M] Writing synchronous replica for gap transaction {}", transactionId);
        }
        Collection<RemoteKafkaProducer> producers = replicaProducersManager.getProducers();

        if (!producers.isEmpty()) {
            List<Future<RecordMetadata>> futures = new ArrayList<>(producers.size());
            for (RemoteKafkaProducer producer : producers) {
                futures.add(producer.writeGapTransaction(transactionId));
            }
            wait(futures);
        }
    }

    private void wait(List<Future<RecordMetadata>> futures) {
        try {
            for (Future<RecordMetadata> future : futures) {
                future.get();
            }
        }
        catch (InterruptedException | ExecutionException e) {
            throw new CacheWriterException(e);
        }
    }
}
