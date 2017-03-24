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

package org.apache.ignite.activestore.impl.kv;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import com.google.common.primitives.Longs;
import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import javax.inject.Inject;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Aleksandr_Meterko
 * @since 10/26/2016
 */
public class TransactionalKafkaProducer {

    private final String dataTopic;
    private final int partitions;
    private final Producer producer;

    @Inject
    private transient Serializer serializer;

    public TransactionalKafkaProducer(String dataTopic, int partitions, Producer producer) {
        this.dataTopic = dataTopic;
        this.partitions = partitions;
        this.producer = producer;
    }

    // TODO do we need metadata?
    @SuppressWarnings("unchecked")
    public void writeTransaction(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates,
        final Metadata metadata) throws CacheWriterException {
        try {
            Message message = convertToMessage(transactionId, updates);
            Object key = serializer.serialize(message.metadata);
            Object value = serializer.serialize(message.values);
            producer.send(new ProducerRecord(dataTopic, partition(message), key, value));
        }
        catch (Exception e) {
            throw new CacheWriterException(e);
        }
    }

    private Message convertToMessage(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates) {
        Message result = new Message(transactionId);
        for (Map.Entry<String, Collection<Cache.Entry<?, ?>>> cacheToUpdates : updates.entrySet()) {
            String cacheName = cacheToUpdates.getKey();
            for (Cache.Entry<?, ?> cacheUpdates : cacheToUpdates.getValue()) {
                result.metadata.addRow(cacheName, cacheUpdates.getKey());
                result.values.add(cacheUpdates.getValue());
            }
        }
        return result;
    }

    private Integer partition(Message msg) {
        return Longs.hashCode(msg.metadata.getTransactionId()) % partitions;
    }

    private static class Message {
        public final MessageMetadata metadata;
        public final List<Object> values;

        Message(long transactionId) {
            metadata = new MessageMetadata(transactionId);
            values = new ArrayList<>();
        }
    }

}
