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

package org.apache.ignite.activestore.impl.cassandra;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.configuration.Factory;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.KeyValueManager;
import org.apache.ignite.activestore.KeyValueProvider;
import org.apache.ignite.activestore.KeyValueReader;
import org.apache.ignite.activestore.MetadataManager;
import org.apache.ignite.activestore.MetadataProvider;
import org.apache.ignite.activestore.impl.ActiveStoreConfiguration;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.activestore.impl.KeyValueManagerImpl;
import org.apache.ignite.activestore.impl.MetadataProviderImpl;
import org.apache.ignite.activestore.impl.cassandra.datasource.DataSource;
import org.apache.ignite.activestore.impl.cassandra.persistence.KeyValuePersistenceSettingsRegistry;
import org.apache.ignite.activestore.impl.cassandra.persistence.PublicKeyValuePersistenceSettings;
import org.apache.ignite.activestore.impl.cassandra.persistence.PublicKeyspacePersistenceSettings;
import org.apache.ignite.activestore.impl.export.FileExporter;
import org.apache.ignite.activestore.impl.kv.SnapshotAwareKeyValueReaderListener;
import org.apache.ignite.cache.store.cassandra.serializer.JavaSerializer;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;

import static javax.cache.configuration.FactoryBuilder.factoryOf;

/**
 * Configuration class which allows user to wire all classes for Cassandra-based store.
 */
public class CassandraActiveStoreConfiguration extends ActiveStoreConfiguration {

    /**
     * Source to for obtaining connection to Cassandra.
     */
    private transient DataSource dataSource;

    /**
     * Factory of sources to obtain connection to Cassandra.
     */
    private transient Factory<DataSource> dataSourceFactory;

    /**
     * Default settings for storing data in Cassandra.
     */
    private transient PublicKeyspacePersistenceSettings keyspacePersistenceSettings;

    /**
     * Specific settings for each cache to store data in Cassandra.
     */
    private transient Map<String, PublicKeyValuePersistenceSettings> settingsByCache;

    /**
     * Sets datasource to use.
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Sets datasource factory to use.
     */
    public void setDataSourceFactory(Factory<DataSource> dataSourceFactory) {
        this.dataSourceFactory = dataSourceFactory;
    }

    /**
     * Sets default settings to use.
     */
    public void setKeyspacePersistenceSettings(PublicKeyspacePersistenceSettings keyspacePersistenceSettings) {
        this.keyspacePersistenceSettings = keyspacePersistenceSettings;
    }

    /**
     * Sets specific settings to use.
     */
    public void setSettingsByCache(Map<String, PublicKeyValuePersistenceSettings> settingsByCache) {
        this.settingsByCache = settingsByCache;
    }

    /** {@inheritDoc} */
    @Override public void afterPropertiesSet() throws Exception {
        if ((dataSource == null && dataSourceFactory == null) || keyspacePersistenceSettings == null) {
            throw new IllegalArgumentException("Both dataSource and keyspacePersistenceSettings must be set for " +
                "CassandraActiveStoreConfiguration");
        }
        if (settingsByCache == null) {
            settingsByCache = new HashMap<>();
        }

        bindings.put(Exporter.class, FileExporter.class);
        bindings.put(Serializer.class, JavaSerializer.class);
        bindings.put(KeyValueManager.class, KeyValueManagerImpl.class);
        bindings.put(MetadataProvider.class, MetadataProviderImpl.class);

        bindings.put(MetadataManager.class, CassandraMetadataManager.class);
        bindings.put(KeyValueProvider.class, CassandraKeyValueProvider.class);
        bindings.put(KeyValueReader.class, SnapshotAwareKeyValueReaderListener.class);

        factories.put(KeyValuePersistenceSettingsRegistry.class,
            factoryOf(new KeyValuePersistenceSettingsRegistry(keyspacePersistenceSettings, settingsByCache)));
        factories.put(DataSource.class, dataSourceFactory == null ?
            factoryOf(dataSource)
            :
            dataSourceFactory);

        List classes = Collections.singletonList(SnapshotAwareKeyValueReaderListener.class);
        factories.put(List.class, new Injection.ListOf<>(classes));
    }
}
