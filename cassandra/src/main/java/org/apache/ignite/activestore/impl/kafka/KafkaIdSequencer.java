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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.cache.integration.CacheWriterException;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Evgeniy_Ignatiev
 * @since 11/25/2016 4:05 PM
 */
public class KafkaIdSequencer implements IdSequencer {
    private final String topic;

    private final Producer producer;

    public KafkaIdSequencer(String topic, Producer producer) {
        this.topic = topic;
        this.producer = producer;
    }

    @SuppressWarnings("unchecked")
    @Override public long getNextId() {
        try {
            Future<RecordMetadata> future = producer.send(new ProducerRecord(topic, 0, null, null));
            RecordMetadata metadata = future.get();
            return metadata.offset();
        } catch (ExecutionException | InterruptedException e) {
            throw new CacheWriterException(e);
        }
    }
}
