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

import java.util.Properties;
import org.apache.ignite.activestore.impl.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

/**
 * @author Andrei_Yakushin
 * @since 12/22/2016 11:08 AM
 */
public class KafkaFactoryImpl implements KafkaFactory {
    @Override
    public <K, V> Producer<K, V> producer(Properties properties) {
        return new KafkaProducer<>(properties);
    }

    @Override
    public <K, V> Producer<K, V> producer(Properties properties, Runnable onStop) {
        return new ProducerProxyRetry<>(this.<K, V>producer(properties), onStop);
    }

    @Override
    public <K, V> Consumer<K, V> consumer(Properties properties) {
        return new KafkaConsumer<>(properties);
    }

    @Override
    public <K, V> Consumer<K, V> consumer(Properties properties, Runnable onStop) {
        return new ConsumerProxyRetry<>(this.<K, V>consumer(properties), onStop);
    }
}
