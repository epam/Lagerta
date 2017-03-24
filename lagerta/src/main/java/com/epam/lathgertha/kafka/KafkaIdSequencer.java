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

import com.epam.lathgertha.capturer.IdSequencer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.cache.integration.CacheWriterException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaIdSequencer implements IdSequencer {
    private final String topic;
    private final Producer producer;

    public KafkaIdSequencer(String topic, KafkaFactory kafkaFactory, SubscriberConfig subscriberConfig) {
        this.topic = topic;
        this.producer = kafkaFactory.producer(subscriberConfig.getProducerConfig());
    }

    @SuppressWarnings("unchecked")
    @Override public long getNextId() {
        try {
            Future<RecordMetadata> future = producer.send(new ProducerRecord(topic, 0, null, null));
            return future.get().offset();
        } catch (ExecutionException | InterruptedException e) {
            throw new CacheWriterException(e);
        }
    }
}
