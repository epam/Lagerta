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

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import javax.cache.Cache;
import org.apache.ignite.activestore.impl.transactions.TransactionMessage;
import org.apache.ignite.activestore.impl.transactions.TransactionMessageBuilder;
import org.apache.ignite.activestore.impl.transactions.TransactionMessageUtil;
import org.apache.ignite.activestore.commons.serializer.Serializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Aleksandr_Meterko
 * @since 10/26/2016
 */
abstract class TransactionalKafkaProducer implements Closeable {
    protected final Producer producer;
    private final Serializer serializer;

    TransactionalKafkaProducer(Producer producer, Serializer serializer) {
        this.producer = producer;
        this.serializer = serializer;
    }

    @SuppressWarnings("unchecked")
    protected Future<RecordMetadata> send(String topic, int partitions, long transactionId,
                                          Map<String, Collection<Cache.Entry<?, ?>>> updates) {
        TransactionMessage message = convertToMessage(transactionId, updates);
        Object key = serializer.serialize(message.metadata);
        Object value = serializer.serialize(message.values);
        ProducerRecord record = makeRecord(transactionId, topic,
            TransactionMessageUtil.partitionFor(message.metadata, partitions), key, value);
        return producer.send(record);
    }

    private TransactionMessage convertToMessage(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates) {
        TransactionMessageBuilder messageBuilder = new TransactionMessageBuilder();

        messageBuilder.setTransactionId(transactionId);
        for (Map.Entry<String, Collection<Cache.Entry<?, ?>>> cacheToUpdates : updates.entrySet()) {
            messageBuilder.addCacheEntries(cacheToUpdates.getKey(), cacheToUpdates.getValue());
        }
        return messageBuilder.build();
    }

    protected abstract ProducerRecord makeRecord(long transactionId, String topic, int partition, Object key, Object value);

    @Override public void close() {
        producer.close();
    }
}
