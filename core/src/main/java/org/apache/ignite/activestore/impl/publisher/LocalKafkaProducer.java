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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import javax.inject.Inject;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.commons.serializer.Serializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/7/2016 7:30 PM
 */
public class LocalKafkaProducer extends TransactionalKafkaProducer {
    private final String localTopic;
    private final int partitions;

    @Inject
    public LocalKafkaProducer(DataRecoveryConfig dataRecoveryConfig, Serializer serializer, KafkaFactory kafkaFactory) {
        super(
                kafkaFactory.producer(dataRecoveryConfig.getProducerConfig(), new Runnable() {
                    @Override
                    public void run() {
                        //todo add logic
                    }
                }),
                serializer
        );
        localTopic = dataRecoveryConfig.getLocalTopic();
        partitions = producer.partitionsFor(localTopic).size();
    }

    public Future<RecordMetadata> writeTransaction(long transactionId, Map<String,
            Collection<Cache.Entry<?, ?>>> updates) throws CacheWriterException {
        try {
            return send(localTopic, partitions, transactionId, updates);
        }
        catch (Exception e) {
            throw new CacheWriterException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override protected ProducerRecord makeRecord(long transactionId, String topic, int partition, Object key,
        Object value) {
        return new ProducerRecord(topic, partition, transactionId, key, value);
    }
}

