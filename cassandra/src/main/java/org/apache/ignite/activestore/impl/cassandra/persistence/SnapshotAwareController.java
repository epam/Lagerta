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

import java.util.ArrayList;
import java.util.List;
import com.datastax.driver.core.Row;
import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.ActiveCacheStore;
import org.apache.ignite.cache.store.cassandra.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.PersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.PersistenceStrategy;
import org.apache.ignite.cache.store.cassandra.persistence.PojoField;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;

public class SnapshotAwareController extends PersistenceController {
    /**
     * Constructs persistence controller from Ignite cache persistence settings.
     *
     * @param settings persistence settings.
     */
    public SnapshotAwareController(KeyValuePersistenceSettings settings) {
        super(settings);
    }

    /** {@inheritDoc} */
    @Override protected Object buildObject(Row row, PersistenceSettings settings) {
        if (row == null) {
            return null;
        }
        PersistenceStrategy stgy = settings.getStrategy();
        Class clazz = settings.getJavaClass();
        String col = settings.getColumn();
        List<PojoField> fields = settings.getFields();
        if (PersistenceStrategy.PRIMITIVE.equals(stgy)) {
            return isTombstone(row, col) ?
                ActiveCacheStore.TOMBSTONE :
                PropertyMappingHelper.getCassandraColumnValue(row, col, clazz, null);
        }
        if (PersistenceStrategy.BLOB.equals(stgy)) {
            return isTombstone(row, settings.getSerializer(), col) ?
                ActiveCacheStore.TOMBSTONE :
                settings.getSerializer().deserialize(row.getBytes(col));
        }
        if (isTombstone(row, fields)) {
            return ActiveCacheStore.TOMBSTONE;
        }
        Object obj;
        try {
            obj = clazz.newInstance();
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to instantiate object of type '" + clazz.getName() +
                "' using reflection", e);
        }
        for (PojoField field : fields) {
            field.setValueFromRow(row, obj, settings.getSerializer());
        }
        return obj;
    }

    /**
     * Checks whether presented row is null (contains tombstone) for specified column.
     *
     * @param row to check.
     * @param column name of column.
     * @return true if contains tombstone.
     */
    private boolean isTombstone(Row row, String column) {
        return isTombstone(row, null, column);
    }

    /**
     * Checks whether presented row is null (contains tombstone) for specified fields.
     *
     * @param row to check.
     * @param fields fields of POJO instances.
     * @return true if contains tombstone.
     */
    private boolean isTombstone(Row row, List<PojoField> fields) {
        List<String> columns = new ArrayList<>(fields.size());
        for (PojoField field : fields) {
            columns.add(field.getColumn());
        }
        return isTombstone(row, null, columns.toArray(new String[0]));
    }

    /**
     * Checks whether presented row is null (contains tombstone) for specified columns.
     *
     * @param row to check.
     * @param serializer which can work with format of this row.
     * @param columns to check nulls.
     * @return true if contains tombstone.
     */
    private boolean isTombstone(Row row, Serializer serializer, String... columns) {
        for (String column : columns) {
            Object obj = row.getObject(column);
            if (obj == null) {
                continue;
            }
            obj = serializer == null ? obj : serializer.deserialize(row.getBytes(column));
            return obj == null || ActiveCacheStore.TOMBSTONE.equals(obj);
        }
        return true;
    }
}

