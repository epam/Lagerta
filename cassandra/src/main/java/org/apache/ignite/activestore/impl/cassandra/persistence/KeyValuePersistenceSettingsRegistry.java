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

package org.apache.ignite.activestore.impl.cassandra.persistence;

import java.io.Serializable;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.impl.cassandra.SnapshotHelper;
import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.ignite.cache.store.cassandra.persistence.KeyPersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.ValuePersistenceSettings;
import org.apache.ignite.internal.util.GridUnsafe;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 * Holder class which keeps all Cassandra-based settings for storing data.
 */
public class KeyValuePersistenceSettingsRegistry implements Serializable {

    /**
     * Default setting for storing keys.
     */
    private final KeyPersistenceSettings defaultKeySettings;

    /**
     * Default settings for storing values.
     */
    private final ValuePersistenceSettings defaultValueSettings;

    /**
     * Default settings for keyspace.
     */
    private final PublicKeyspacePersistenceSettings generalSettings;

    /**
     * Specific setting of storing data by cache name.
     */
    private final Map<String, PublicKeyValuePersistenceSettings> cacheSettings;

    /**
     * Cached settings for specific Ignite caches.
     */
    private transient Map<String, KeyValuePersistenceSettings> cache;

    /**
     * Cretes registry by specifying all settings.
     *
     * @param generalSettings basic settings to be used.
     * @param cacheSettings specific settings by cache.
     */
    public KeyValuePersistenceSettingsRegistry(PublicKeyspacePersistenceSettings generalSettings,
        Map<String, PublicKeyValuePersistenceSettings> cacheSettings) {
        this.generalSettings = generalSettings;
        this.cacheSettings = cacheSettings;

        String serializer = generalSettings.getSerializer().getClass().getName();
        defaultKeySettings = new KeyPersistenceSettings(parse("<keyPersistence strategy=\"BLOB\" serializer=\"" +
            serializer + "\"/>"));
        defaultValueSettings = new ValuePersistenceSettings(parse("<valuePersistence strategy=\"BLOB\" serializer=\"" +
            serializer + "\"/>"));
    }

    /**
     * Parses xml from String.
     *
     * @param xml String containing xml.
     * @return XML DOM element.
     */
    private static Element parse(String xml) {
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            return builder.parse(new InputSource(new StringReader(xml))).getDocumentElement();
        }
        catch (Throwable e) {
            throw new IllegalArgumentException("Failed to parse persistence settings:" +
                SystemHelper.LINE_SEPARATOR + xml, e);
        }
    }

    /**
     * Implementation of so-called elvis operator.
     *
     * @param baseValue default value.
     * @param replacementValue value if baseValue is null.
     * @param <E> any type.
     * @return non-null value (if present among parameters).
     */
    private static <E> E elvis(E baseValue, E replacementValue) {
        return baseValue == null ? replacementValue : baseValue;
    }

    /**
     * Sets inner field for object using reflection.
     *
     * @param instance of any object.
     * @param value to be set into this object.
     * @param fieldName existing field of object.
     * @throws NoSuchFieldException if field was not found.
     * @throws IllegalAccessException if security restrictions are applied.
     */
    private static void set(Object instance, Object value, String fieldName) throws NoSuchFieldException,
        IllegalAccessException {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(instance, value);
    }

    /**
     * Returns name of keyspace which is managed by this registry.
     *
     * @return keyspace.
     */
    public String getKeyspace() {
        return generalSettings.getKeyspace();
    }

    /**
     * Obtains controller for specific cache.
     *
     * @param cacheName Ignite cache name.
     * @param metadata snapshot for which settings are needed.
     * @return PersistenceController
     */
    public PersistenceController get(String cacheName, Metadata metadata) {
        return new SnapshotAwareController(getSettings(cacheName, metadata));
    }

    /**
     * Obtains controller for specific table.
     *
     * @param tableName name of table in Cassandra
     * @return PersistenceController
     */
    public PersistenceController get(String tableName) {
        return new SnapshotAwareController(getSettings(tableName, tableName));
    }

    /**
     * Obtains settings for specific table.
     *
     * @param cacheName Ignite cache name.
     * @param metadata snapshot to determine table.
     * @return settings for target Cassandra table.
     */
    private KeyValuePersistenceSettings getSettings(String cacheName, Metadata metadata) {
        String tableName = SnapshotHelper.getTableName(cacheName, metadata);
        return getSettings(cacheName, tableName);
    }

    /**
     * Obtains settings for specific table.
     *
     * @param cacheName Ignite cache name.
     * @param tableName Cassandra table name.
     * @return settings for target Cassandra table.
     */
    private KeyValuePersistenceSettings getSettings(String cacheName, String tableName) {
        Map<String, KeyValuePersistenceSettings> cache = this.cache == null ? this.cache = new HashMap<>() : this.cache;
        KeyValuePersistenceSettings settings = cache.get(tableName);
        if (settings == null) {
            cache.put(tableName, settings = create(cacheName, tableName));
        }
        return settings;
    }

    /**
     * Creates settings for specific table.
     *
     * @param cacheName Ignite cache name.
     * @param tableName Cassandra table name.
     * @return settings for target Cassandra table.
     */
    private KeyValuePersistenceSettings create(String cacheName, String tableName) {
        PublicKeyValuePersistenceSettings keyValue = cacheSettings.get(cacheName);
        if (keyValue == null) {
            keyValue = new PublicKeyValuePersistenceSettings(null, null, defaultKeySettings, defaultValueSettings);
            cacheSettings.put(cacheName, keyValue);
        }
        try {
            Object instance = GridUnsafe.allocateInstance(KeyValuePersistenceSettings.class);
            set(instance, elvis(keyValue.getTTL(), generalSettings.getTTL()), "ttl");
            set(instance, generalSettings.getKeyspace(), "keyspace");
            set(instance, tableName, "tbl");
            set(instance, elvis(keyValue.getTableOptions(), generalSettings.getTableOptions()), "tblOptions");
            set(instance, generalSettings.getKeyspaceOptions(), "keyspaceOptions");
            set(instance, elvis(keyValue.getKeyPersistenceSettings(), defaultKeySettings), "keyPersistenceSettings");
            set(instance, elvis(keyValue.getValuePersistenceSettings(), defaultValueSettings), "valPersistenceSettings");
            return (KeyValuePersistenceSettings)instance;
        }
        catch (InstantiationException | NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Cannot create Settings");
        }
    }
}
