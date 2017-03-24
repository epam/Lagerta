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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.activestore.impl.config.UnsubscriberOnFailWrapper;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.commons.serializer.Serializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/7/2016 7:30 PM
 */
public class RemoteKafkaProducer extends TransactionalKafkaProducer {
    private final UnsubscriberOnFailWrapper unsubscriberOnFailWrapper;

    private final String remoteTopic;
    private final int remotePartitions;

    private final String reconTopic;
    private final int reconPartitions;

    public RemoteKafkaProducer(ReplicaConfig config, Serializer serializer, KafkaFactory kafkaFactory, UnsubscriberOnFailWrapper unsubscriberOnFailWrapper) {
        super(kafkaFactory.producer(config.getProducerConfig()), serializer);
        this.unsubscriberOnFailWrapper = unsubscriberOnFailWrapper;

        remoteTopic = config.getRemoteTopic();
        remotePartitions = producer.partitionsFor(remoteTopic).size();

        reconTopic = config.getReconciliationTopic();
        reconPartitions = producer.partitionsFor(reconTopic).size();
    }

    public Future<RecordMetadata> writeTransaction(long transactionId, Map<String,
            Collection<Cache.Entry<?, ?>>> updates) throws CacheWriterException {
        try {
            return unsubscriberOnFailWrapper.wrap(send(remoteTopic, remotePartitions, transactionId, updates));
        }
        catch (Exception e) {
            return unsubscriberOnFailWrapper.empty(e);
        }
    }

    public Future<RecordMetadata> writeGapTransaction(long transactionId) throws CacheWriterException {
        try {
            return unsubscriberOnFailWrapper.wrap(send(reconTopic, reconPartitions, transactionId,
                    Collections.<String, Collection<Cache.Entry<?, ?>>>emptyMap()));
        }
        catch (Exception e) {
            return unsubscriberOnFailWrapper.empty(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override protected ProducerRecord makeRecord(long transactionId, String topic, int partition, Object key, Object value) {
        return new ProducerRecord(topic, partition, key, value);
    }
}
