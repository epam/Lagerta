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

package org.apache.ignite.activestore.impl.kafka;

import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.integration.CacheWriterException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Evgeniy_Ignatiev
 * @since 11/25/2016 4:05 PM
 */
public class KafkaIdSequencer implements IdSequencer, LifecycleAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIdSequencer.class);

    private static final int SEQUENCE_PARTITION = 0;

    private final TopicPartition topic;
    private final Producer producer;
    private final Consumer consumer;

    public KafkaIdSequencer(String topic, Producer producer, Consumer consumer) {
        this.topic = new TopicPartition(topic, SEQUENCE_PARTITION);
        this.producer = producer;
        this.consumer = consumer;
    }

    @SuppressWarnings("unchecked")
    @Override public long getNextId() {
        try {
            Future<RecordMetadata> future = producer.send(new ProducerRecord(topic.topic(), topic.partition(), null, null));
            RecordMetadata metadata = future.get();
            return metadata.offset();
        } catch (ExecutionException | InterruptedException e) {
            throw new CacheWriterException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override public long getLastProducedId() {
        Map<TopicPartition, Long> result = consumer.endOffsets(Collections.singletonList(topic));
        return result.get(topic) - 1;
    }

    @Override public void start() {
        consumer.subscribe(Collections.singletonList(topic.topic()));
    }

    @Override public void stop() {
        try {
            producer.close();
        } catch (Exception e) {
            LOGGER.error("[M] Exception while closing producer: ", e);
        }
        try {
            consumer.close();
        } catch (Exception e) {
            LOGGER.error("[M] Exception while closing consumer: ", e);
        }
    }
}
