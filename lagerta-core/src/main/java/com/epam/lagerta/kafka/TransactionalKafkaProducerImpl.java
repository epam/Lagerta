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

import com.epam.lagerta.capturer.KeyTransformer;
import com.epam.lagerta.capturer.TransactionScope;
import com.epam.lagerta.capturer.TransactionalProducer;
import com.epam.lagerta.capturer.ValueTransformer;
import com.epam.lagerta.kafka.config.SubscriberConfig;
import com.epam.lagerta.util.Serializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static com.epam.lagerta.util.TransactionPartitionUtil.partition;

public class TransactionalKafkaProducerImpl implements TransactionalProducer {
    private final String dataTopic;
    private final int partitions;
    private final Producer producer;
    private final KeyTransformer keyTransformer;
    private final ValueTransformer valueTransformer;
    private final Serializer serializer;

    public TransactionalKafkaProducerImpl(SubscriberConfig subscriberConfig, KafkaFactory kafkaFactory,
                                          KeyTransformer keyTransformer, ValueTransformer valueTransformer, Serializer serializer) {
        dataTopic = subscriberConfig.getInputTopic();
        this.keyTransformer = keyTransformer;
        this.valueTransformer = valueTransformer;
        producer = kafkaFactory.producer(subscriberConfig.getProducerConfig());
        this.serializer = serializer;
        partitions = producer.partitionsFor(dataTopic).size();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Future<RecordMetadata> send(long transactionId,
        Map<String, Collection<Cache.Entry<?, ?>>> updates) throws CacheWriterException {
        try {
            int partition = partition(transactionId, partitions);
            TransactionScope key = keyTransformer.apply(transactionId, updates);
            List<List> value = valueTransformer.apply(updates);
            ProducerRecord record = new ProducerRecord(dataTopic, partition, transactionId, serializer.serialize(key),
                serializer.serialize(value));
            return producer.send(record);
        }
        catch (Exception e) {
            throw new CacheWriterException(e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
