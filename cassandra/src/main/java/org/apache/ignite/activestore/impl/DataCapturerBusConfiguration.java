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

package org.apache.ignite.activestore.impl;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.List;
import java.util.Properties;

import javax.cache.configuration.Factory;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.KeyValueListener;
import org.apache.ignite.activestore.KeyValueManager;
import org.apache.ignite.activestore.KeyValueProvider;
import org.apache.ignite.activestore.KeyValueReader;
import org.apache.ignite.activestore.MetadataManager;
import org.apache.ignite.activestore.MetadataProvider;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.activestore.impl.export.FileExporter;
import org.apache.ignite.activestore.impl.kv.KafkaProducers;
import org.apache.ignite.activestore.impl.kv.SnapshotAwareKeyValueReaderListener;
import org.apache.ignite.activestore.impl.kv.TransactionalKafkaKVProvider;
import org.apache.ignite.activestore.impl.kv.TransactionalKafkaProducer;
import org.apache.ignite.activestore.impl.util.PropertiesUtil;
import org.apache.ignite.cache.store.cassandra.serializer.JavaSerializer;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.util.Assert;

/**
 * @author Andrei_Yakushin
 * @since 10/31/2016 12:43 PM
 */
public class DataCapturerBusConfiguration extends ActiveStoreConfiguration {
    public static final String DATA_TOPIC = "data.capturer.topic";
    public static final String NUMBER_OF_PARTITIONS = "data.capturer.partitions";

    private List<Class<? extends KeyValueListener>> listeners;
    private Properties replicationProperties;

    public void setListeners(List<Class<? extends KeyValueListener>> listeners) {
        this.listeners = listeners;
    }

    public void setReplicationProperties(Properties replicationProperties) {
        this.replicationProperties = replicationProperties;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(replicationProperties);
        Assert.notEmpty(listeners);
        bindings.put(Exporter.class, FileExporter.class);
        bindings.put(Serializer.class, JavaSerializer.class);
        bindings.put(KeyValueManager.class, KeyValueManagerImpl.class);
        bindings.put(MetadataProvider.class, MetadataProviderImpl.class);
        bindings.put(MetadataManager.class, InMemoryMetadataManager.class);
        bindings.put(KeyValueReader.class, SnapshotAwareKeyValueReaderListener.class);
        bindings.put(KeyValueProvider.class, TransactionalKafkaKVProvider.class);

        factories.put(List.class, new Injection.ListOf<>(listeners));
        namedFactories.put(
            new AbstractMap.SimpleImmutableEntry<Class, String>(
                TransactionalKafkaProducer.class, KafkaProducers.SNAPSHOTS),
            new TransactionalKafkaProducerFactory(replicationProperties)
        );
    }

    private static class TransactionalKafkaProducerFactory implements Serializable, Factory<TransactionalKafkaProducer> {
        private final Properties properties;

        TransactionalKafkaProducerFactory(Properties properties) {
            this.properties = properties;
        }

        @Override
        public TransactionalKafkaProducer create() {
            String dataTopic = properties.getProperty(DATA_TOPIC);
            int partitions = Integer.parseInt(properties.getProperty(NUMBER_OF_PARTITIONS));
            Properties producerProperties = PropertiesUtil.excludeProperties(properties, DATA_TOPIC,
                NUMBER_OF_PARTITIONS);

            return new TransactionalKafkaProducer(dataTopic, partitions, new KafkaProducer(producerProperties));
        }
    }
}
