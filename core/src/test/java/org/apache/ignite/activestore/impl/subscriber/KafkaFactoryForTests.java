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

package org.apache.ignite.activestore.impl.subscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.ignite.activestore.impl.kafka.KafkaFactoryImpl;
import org.apache.ignite.activestore.impl.subscriber.consumer.ConsumerAdapter;
import org.apache.ignite.activestore.impl.subscriber.consumer.ConsumerForTests;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Evgeniy_Ignatiev
 * @since 19:55 12/29/2016
 */
public class KafkaFactoryForTests extends KafkaFactoryImpl {
    private final List<ConsumerForTests> consumers = new ArrayList<>();

    @Override public <K, V> Consumer<K, V> consumer(Properties properties) {
        Consumer<K, V> consumer = super.consumer(properties);
        return wrapConsumer(consumer);
    }

    @Override public <K, V> Consumer<K, V> consumer(Properties properties, Runnable onStop) {
        return super.consumer(properties, onStop);
    }


    private <K, V> Consumer<K, V> wrapConsumer(Consumer<K, V> consumer) {
        ConsumerForTests<K, V, Consumer<K, V>> testConsumer = new ConsumerForTests<>(consumer);

        consumers.add(testConsumer);
        return testConsumer;
    }

    @SuppressWarnings("unchecked")
    public void substituteConsumers(ConsumerAdapter substitute) {
        for (ConsumerForTests consumer : consumers) {
            consumer.setSubstitute(substitute);
        }
    }
}
