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

import com.epam.lagerta.BaseIntegrationTest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaFactoryForTests implements KafkaFactory {
    private final KafkaFactory kafkaFactory;
    private final Properties producerConfig;

    public KafkaFactoryForTests(KafkaFactory kafkaFactory, Properties producerConfig) {
        this.kafkaFactory = kafkaFactory;
        this.producerConfig = producerConfig;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Producer<K, V> producer(Properties properties) {
        return (Producer<K, V>) Proxy.newProxyInstance(
                Producer.class.getClassLoader(),
                new Class[] {Producer.class},
                new ProducerProxy(kafkaFactory.producer(properties))
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Consumer<K, V> consumer(Properties properties) {
        return (Consumer<K, V>) Proxy.newProxyInstance(
                Consumer.class.getClassLoader(),
                new Class[] {Consumer.class},
                new ConsumerProxy(this, kafkaFactory.consumer(properties))
        );
    }

    void ensureTopicsCreated(Collection<String> topics) {
        try (Producer producer = kafkaFactory.producer(producerConfig)) {
            for (String topic : topics) {
                producer.partitionsFor(topic);
            }
        }
    }

    private static class ConsumerProxy implements InvocationHandler {
        private static final String SUBSCRIBE_METHOD_NAME = "subscribe";

        private final KafkaFactoryForTests kafkaFactory;
        private final Consumer consumer;

        ConsumerProxy(KafkaFactoryForTests kafkaFactory, Consumer consumer) {
            this.kafkaFactory = kafkaFactory;
            this.consumer = consumer;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (SUBSCRIBE_METHOD_NAME.equals(method.getName())) {
                Collection<String> topics = ((Collection<String>) args[0])
                        .stream()
                        .map(BaseIntegrationTest::adjustTopicNameForTest)
                        .collect(Collectors.toList());
                args[0] = topics;
                kafkaFactory.ensureTopicsCreated(topics);
            }
            return method.invoke(consumer, args);
        }
    }

    private static class ProducerProxy implements InvocationHandler {
        private static final String SEND_METHOD_NAME = "send";
        private static final String PARTITIONS_FOR_METHOD_NAME = "partitionsFor";

        private final Producer producer;

        ProducerProxy(Producer producer) {
            this.producer = producer;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case SEND_METHOD_NAME: {
                    ProducerRecord record = (ProducerRecord) args[0];

                    args[0] = new ProducerRecord<>(
                            BaseIntegrationTest.adjustTopicNameForTest(record.topic()),
                            record.partition(),
                            record.timestamp(),
                            record.key(),
                            record.value()
                    );
                    break;
                }
                case PARTITIONS_FOR_METHOD_NAME: {
                    args[0] = BaseIntegrationTest.adjustTopicNameForTest((String) args[0]);
                    break;
                }
            }
            return method.invoke(producer, args);
        }
    }
}
