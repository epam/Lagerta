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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class KafkaFactoryImpl implements KafkaFactory {
    private final Queue<Closeable> closeables = new ConcurrentLinkedQueue<>();

    public void closeAll() {
        while (!closeables.isEmpty())
            try {
                closeables.poll().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    @Override
    public <K, V> Producer<K, V> producer(Properties properties) {
        return add(new KafkaProducer<>(properties));
    }

    @Override
    public <K, V> Consumer<K, V> consumer(Properties properties) {
        return add(new KafkaConsumer<>(properties));
    }

    private <T extends Closeable> T add(T t) {
        closeables.add(t);
        return t;
    }
}
