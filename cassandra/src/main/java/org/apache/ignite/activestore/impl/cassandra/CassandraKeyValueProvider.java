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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.inject.Inject;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.activestore.ActiveCacheStore;
import org.apache.ignite.activestore.KeyValueProvider;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.impl.cassandra.datasource.DataSource;
import org.apache.ignite.activestore.impl.cassandra.persistence.KeyValuePersistenceSettingsRegistry;
import org.apache.ignite.activestore.impl.cassandra.persistence.PersistenceController;
import org.apache.ignite.activestore.impl.cassandra.session.CassandraSession;
import org.apache.ignite.activestore.impl.cassandra.session.TransactionExecutionAssistance;
import org.apache.ignite.activestore.impl.cassandra.session.adapter.GenericBatchExecutionAssistant;
import org.apache.ignite.activestore.impl.cassandra.session.adapter.GenericExecutionAssistant;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.resources.LoggerResource;

/**
 * Provides loading and saving keys from/to Cassandra storage.
 */
public class CassandraKeyValueProvider implements KeyValueProvider {
    /**
     * Source for obtaining connection to Cassandra
     */
    @Inject
    private transient DataSource dataSrc;

    /**
     * Settings for storing data in Cassandra.
     */
    @Inject
    private transient KeyValuePersistenceSettingsRegistry settings;

    /**
     * Logger
     */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> V load(final K key, String cacheName,
        Iterable<Metadata> path) throws CacheLoaderException {
        if (key == null) {
            return null;
        }
        CassandraSession ses = session();
        try {
            for (Metadata metadata : path) {
                final PersistenceController ctrl = settings.get(cacheName, metadata);
                Object val = ses.execute(new GenericExecutionAssistant(false, "READ") {
                    @Override public String getStatement() {
                        return ctrl.getLoadStatement(false, true, true);
                    }

                    @Override public BoundStatement bindStatement(PreparedStatement statement) {
                        return ctrl.bindKey(statement, key);
                    }

                    @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                        return ctrl.getPersistenceSettings();
                    }

                    @Override public Object process(Row row) {
                        return row == null ? null : ctrl.buildValueObject(row);
                    }
                });
                if (val != null) {
                    return ActiveCacheStore.TOMBSTONE.equals(val) ? null : (V)val;
                }
            }
        }
        finally {
            U.closeQuiet(ses);
        }
        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
    @Override public <K, V> Map<K, V> loadAll(Iterable<? extends K> keys, String cacheName,
        Iterable<Metadata> path) throws CacheLoaderException {
        if (keys == null || !keys.iterator().hasNext()) {
            return null;
        }
        Iterable keysToProcess = keys;
        final Set tombstones = new HashSet();
        final Map<K, V> loaded = new HashMap<>();
        CassandraSession ses = session();
        try {
            for (Metadata metadata : path) {
                final PersistenceController ctrl = settings.get(cacheName, metadata);
                Map entries = ses.execute(new GenericBatchExecutionAssistant<Map, Object>(false, "BULK_READ") {
                    @Override public String getStatement() {
                        return ctrl.getLoadStatement(true, true, true);
                    }

                    @Override public BoundStatement bindStatement(PreparedStatement statement, Object key) {
                        return ctrl.bindKey(statement, key);
                    }

                    @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                        return ctrl.getPersistenceSettings();
                    }

                    @Override public Map processedData() {
                        return loaded;
                    }

                    @Override protected void process(Row row) {
                        Object key = ctrl.buildKeyObject(row);
                        Object val = ctrl.buildValueObject(row);

                        if (ActiveCacheStore.TOMBSTONE.equals(val)) {
                            tombstones.add(key);
                        }
                        else {
                            loaded.put((K)key, (V)val);
                        }
                    }
                }, keysToProcess);
                if (entries != null && entries.isEmpty()) {
                    continue;
                }
                List nextRoundKeys = new LinkedList();
                for (Object key : keysToProcess) {
                    if (!tombstones.contains(key) && !loaded.containsKey(key)) {
                        nextRoundKeys.add(key);
                    }
                }
                if (nextRoundKeys.isEmpty()) {
                    break;
                }
                keysToProcess = nextRoundKeys;
            }
        }
        finally {
            U.closeQuiet(ses);
        }
        return loaded;
    }

    /** {@inheritDoc} */
    @Override public void write(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates,
                                Metadata metadata) throws CacheWriterException {
        if (updates == null || updates.isEmpty()) {
            return;
        }
        final Map<String, PersistenceController> ctrl = new HashMap<>(updates.size());
        for (String cacheName : updates.keySet()) {
            ctrl.put(cacheName, settings.get(cacheName, metadata));
        }
        CassandraSession ses = session();
        try {
            ses.execute(new TransactionExecutionAssistance() {
                @Override public boolean tableExistenceRequired() {
                    return true;
                }

                @Override public KeyValuePersistenceSettings getPersistenceSettings(String cacheName) {
                    return ctrl.get(cacheName).getPersistenceSettings();
                }

                @Override public String getStatement(String cacheName) {
                    return ctrl.get(cacheName).getWriteStatement();
                }

                @Override
                public BoundStatement bindStatement(String cacheName, PreparedStatement statement, Cache.Entry entry) {
                    return ctrl.get(cacheName).bindKeyValue(statement, entry.getKey(), entry.getValue());
                }
            }, updates);
        }
        finally {
            U.closeQuiet(ses);
        }
    }

    /** {@inheritDoc} */
    @Override public void fetchAllKeys(String cacheName, Metadata metadata,
        final IgniteInClosure<Object> action) throws CacheLoaderException {
        final PersistenceController ctrl = settings.get(cacheName, metadata);
        CassandraSession ses = session();
        try {
            ses.executeAllRows(new GenericExecutionAssistant<Void>(false, "READ ALL KEYS") {
                @Override public String getStatement() {
                    return ctrl.getLoadStatement(true, false, false);
                }

                @Override public BoundStatement bindStatement(PreparedStatement statement) {
                    return new BoundStatement(statement);
                }

                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return ctrl.getPersistenceSettings();
                }

                @Override public Void process(Row row) {
                    action.apply(ctrl.buildKeyObject(row));
                    return null;
                }
            });
        }
        finally {
            U.closeQuiet(ses);
        }
    }

    /** {@inheritDoc} */
    @Override public void fetchAllKeyValues(String cacheName, Metadata metadata,
        final IgniteInClosure<Cache.Entry<Object, Object>> action) {
        CassandraSession ses = session();
        try {
            final PersistenceController ctrl = settings.get(cacheName, metadata);
            ses.executeAllRows(new GenericExecutionAssistant<Void>(false, "READ ALL KEY-VALUES") {
                @Override public String getStatement() {
                    return ctrl.getLoadStatement(true, true, false);
                }

                @Override public BoundStatement bindStatement(PreparedStatement statement) {
                    return new BoundStatement(statement);
                }

                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return ctrl.getPersistenceSettings();
                }

                @Override public Void process(final Row row) {
                    action.apply(new Cache.Entry<Object, Object>() {
                        @Override public Object getKey() {
                            return ctrl.buildKeyObject(row);
                        }

                        @Override public Object getValue() {
                            return ctrl.buildValueObject(row);
                        }

                        @Override public <T> T unwrap(Class<T> clazz) {
                            throw new UnsupportedOperationException();
                        }
                    });
                    return null;
                }
            });
        }
        finally {
            U.closeQuiet(ses);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, List<Metadata>> getSnapshotsByCache(Iterable<Metadata> metadatas) {
        final Map<String, Metadata> subPathNames = new HashMap<>();
        for (Metadata metadata : metadatas) {
            subPathNames.put(SnapshotHelper.getTableName("", metadata), metadata);
        }
        final Map<String, List<Metadata>> result = new HashMap<>();
        CassandraSession ses = session();
        try {
            ses.executeAllRows(new GenericExecutionAssistant<Void>(false, "GET EXISTED CACHE TABLES") {
                @Override public String getStatement() {
                    return "SELECT table_name FROM system_schema.tables WHERE keyspace_name=?";
                }

                @Override public BoundStatement bindStatement(PreparedStatement statement) {
                    BoundStatement boundStatement = new BoundStatement(statement);
                    boundStatement.bind(settings.getKeyspace());
                    return boundStatement;
                }

                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return null;
                }

                @Override public Void process(Row row) {
                    String tableName = row.get("table_name", String.class);
                    if (tableName != null) {
                        for (Map.Entry<String, Metadata> entry : subPathNames.entrySet()) {
                            if (tableName.contains(entry.getKey())) {
                                String cacheName = tableName.substring(0, tableName.length() - entry.getKey().length());
                                List<Metadata> metadatas = result.get(cacheName);
                                if (metadatas == null) {
                                    metadatas = new ArrayList<>();
                                    result.put(cacheName, metadatas);
                                }
                                metadatas.add(entry.getValue());
                                break;
                            }
                        }
                    }
                    return null;
                }
            });
        }
        finally {
            U.closeQuiet(ses);
        }
        for (Map.Entry<String, List<Metadata>> entry : result.entrySet()) {
            List<Metadata> value = entry.getValue();
            List<Metadata> sorted = new ArrayList<>(value.size());
            for (Metadata metadata : metadatas) {
                if (value.contains(metadata)) {
                    sorted.add(metadata);
                }
            }
            entry.setValue(sorted);
        }
        return result;
    }

    /**
     * Onbtains session for connecting to Cassandra.
     *
     * @return session
     */
    private CassandraSession session() {
        return dataSrc.session(log == null ? new NullLogger() : log);
    }
}
