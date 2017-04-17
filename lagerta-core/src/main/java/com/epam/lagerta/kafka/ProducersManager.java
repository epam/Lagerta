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

import com.epam.lagerta.capturer.KeyTransformer;
import com.epam.lagerta.capturer.SuspendableProducer;
import com.epam.lagerta.capturer.TransactionalProducer;
import com.epam.lagerta.capturer.ValueTransformer;
import com.epam.lagerta.util.Serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ProducersManager implements Supplier<List<TransactionalProducer>> {
    private final KafkaFactory kafkaFactory;
    private final KeyTransformer keyTransformer;
    private final ValueTransformer valueTransformer;
    private final Serializer serializer;

    private volatile Producers producers;

    public ProducersManager(KafkaFactory kafkaFactory, KeyTransformer keyTransformer, ValueTransformer valueTransformer,
                            List<SubscriberConfig> configs, Serializer serializer) {
        this.kafkaFactory = kafkaFactory;
        this.keyTransformer = keyTransformer;
        this.valueTransformer = valueTransformer;
        this.serializer = serializer;

        producers = new Producers(configs.size());
        configs.forEach(producers::addProducer);
    }

    @Override
    public List<TransactionalProducer> get() {
        return producers.list();
    }

    public void resume(String subscriberId) {
        ((SuspendableProducer)producers.get(subscriberId)).resume();
    }

    public synchronized void updateConfiguration(List<SubscriberConfig> configs) {
        Producers newProducers = new Producers(configs.size());

        // ToDo: Close unsubscribed producers.
        configs.forEach(config -> newProducers.addProducer(config, producers));
        producers = newProducers;
    }

    private class Producers {
        private final List<TransactionalProducer> producerList;
        private final Map<String, TransactionalProducer> subscriberToProducer;

        Producers(int size) {
            producerList = new ArrayList<>(size);
            subscriberToProducer = new HashMap<>(size);
        }

        List<TransactionalProducer> list() {
            return producerList;
        }

        TransactionalProducer get(String subscriberId) {
            return subscriberToProducer.get(subscriberId);
        }

        void addProducer(SubscriberConfig config) {
            addProducer(config, null);
        }

        void addProducer(SubscriberConfig config, Producers existingProducers) {
            String subscriberId = config.getSubscriberId();
            TransactionalProducer producer = existingProducers == null
                ? null : existingProducers.get(subscriberId);

            if (producer == null) {
                producer = new TransactionalKafkaProducerImpl(
                    config,
                    kafkaFactory,
                    keyTransformer,
                    valueTransformer,
                    serializer
                );
                if (config.isSuspendAllowed()) {
                    producer = new SuspendableKafkaProducerImpl(producer);
                }
            }
            producerList.add(producer);
            subscriberToProducer.put(subscriberId, producer);
        }
    }
}
