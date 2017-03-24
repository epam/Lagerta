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

package org.apache.ignite.activestore.impl.quasi;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

/**
 * @author Evgeniy_Ignatiev
 * @since 10/7/2016 3:44 PM
 */
public class FakeKafkaProducer<K, V> implements Producer<K, V>, Serializable {
    private final long sleepTime;

    public FakeKafkaProducer(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return new SleepFuture(sleepTime);
    }

    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        return new SleepFuture(sleepTime);
    }

    public void flush() {
        // Do nothing.
    }

    public List<PartitionInfo> partitionsFor(String topic) {
        return Collections.emptyList();
    }

    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.emptyMap();
    }

    public void close() {
        // Do nothing.
    }

    public void close(long timeout, TimeUnit unit) {
        // Do nothing.
    }
}
