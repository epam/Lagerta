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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.common.PropertyMappingHelper;
import org.apache.ignite.cache.store.cassandra.persistence.KeyPersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.PersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.PersistenceStrategy;
import org.apache.ignite.cache.store.cassandra.persistence.PojoField;
import org.apache.ignite.cache.store.cassandra.persistence.ValuePersistenceSettings;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;

/**
 * Intermediate layer between persistent store (Cassandra) and Ignite cache key/value classes. Handles  all the mappings
 * to/from Java classes into Cassandra and responsible for all the details of how Java objects should be written/loaded
 * to/from Cassandra.
 */
public class PersistenceController {
    /** Ignite cache key/value persistence settings. */
    private KeyValuePersistenceSettings persistenceSettings;

    private Map<String, String> writeStatements = new HashMap<>();

    /** CQL statement to delete row from Cassandra table. */
    private Map<String, String> delStatements = new HashMap<>();

    /** CQL statement to select value fields from Cassandra table. */
    private Map<String, String> loadValueStatements = new HashMap<>();

    /** CQL statement to select key/value fields from Cassandra table. */
    private Map<String, String> loadKeyValueStatements = new HashMap<>();

    /**
     * Constructs persistence controller from Ignite cache persistence settings.
     *
     * @param settings persistence settings.
     */
    public PersistenceController(KeyValuePersistenceSettings settings) {
        if (settings == null)
            throw new IllegalArgumentException("Persistent settings can't be null");

        this.persistenceSettings = settings;
    }

    /**
     * Returns Ignite cache persistence settings.
     *
     * @return persistence settings.
     */
    public KeyValuePersistenceSettings getPersistenceSettings() {
        return persistenceSettings;
    }

    /**
     * Returns Cassandra keyspace to use.
     *
     * @return keyspace.
     */
    public String getKeyspace() {
        return persistenceSettings.getKeyspace();
    }

    /**
     * Returns Cassandra table to use.
     *
     * @return table.
     */
    public String getTable() {
        return persistenceSettings.getTable();
    }

    /**
     * Returns CQL statement to insert row into Cassandra table.
     *
     * @return CQL statement.
     */
    public String getWriteStatement() {
        return getWriteStatement(getKeyspace(), getTable());
    }

    public String getWriteStatement(String keyspace, String table) {
        String key = keyspace.concat(table);
        String statement = writeStatements.get(key);
        if (statement != null) {
            return statement;
        }
        List<String> cols = getKeyValueColumns();
        StringBuilder colsList = new StringBuilder();
        StringBuilder questionsList = new StringBuilder();
        for (String column : cols) {
            if (colsList.length() != 0) {
                colsList.append(", ");
                questionsList.append(",");
            }
            colsList.append(column);
            questionsList.append("?");
        }
        statement = "insert into " + keyspace + "." + table + " (" +
            colsList.toString() + ") values (" + questionsList.toString() + ")";
        if (persistenceSettings.getTTL() != null) {
            statement += " using ttl " + persistenceSettings.getTTL();
        }
        statement += ";";
        writeStatements.put(key, statement);
        return statement;
    }

    /**
     * Returns CQL statement to delete row from Cassandra table.
     *
     * @return CQL statement.
     */
    public String getDeleteStatement() {
        return getDeleteStatement(getKeyspace(), getTable());
    }

    public String getDeleteStatement(String keyspace, String table) {
        String key = keyspace.concat(table);
        String statement = delStatements.get(key);
        if (statement != null) {
            return statement;
        }
        List<String> cols = getKeyColumns();
        StringBuilder st = new StringBuilder();
        for (String column : cols) {
            if (st.length() != 0) {
                st.append(" and ");
            }
            st.append(column).append("=?");
        }
        st.append(";");
        statement = "delete from " + keyspace + "." + table + " where " + st.toString();
        delStatements.put(key, statement);
        return statement;
    }

    /**
     * Returns CQL statement to select key/value fields from Cassandra table.
     *
     * @param includeKeyFields whether to include/exclude key fields from the returned row.
     * @param includeValueFields whether to include/exclude values fields from the returned row.
     * @param includeFilter whether to include/exclude where clause.
     * @return CQL statement.
     */
    public String getLoadStatement(boolean includeKeyFields, boolean includeValueFields, boolean includeFilter) {
        return getLoadStatement(getKeyspace(), getTable(), includeKeyFields, includeValueFields, includeFilter);
    }

    public String getLoadStatement(String keyspace, String table, boolean includeKeyFields, boolean includeValueFields,
        boolean includeFilter) {
        Map<String, String> map = includeFilter
            ? includeKeyFields
            ? includeValueFields ? loadKeyValueStatements : null
            : includeValueFields ? loadValueStatements : null
            : null;
        String key = keyspace.concat(table);
        if (map != null) {
            String result = map.get(key);
            if (result != null) {
                return result;
            }
        }
        String result = loadStatement(keyspace, table, includeKeyFields, includeValueFields, includeFilter);
        if (map != null) {
            map.put(key, result);
        }
        return result;
    }

    private String loadStatement(String keyspace, String table, boolean includeKeyFields, boolean includeValueFields,
        boolean includeFilter) {
        StringBuilder result = new StringBuilder("select ");
        boolean first = true;
        List<String> keyColumns = getKeyColumns();
        if (includeKeyFields) {
            for (String keyColumn : keyColumns) {
                if (first) {
                    first = false;
                }
                else {
                    result.append(", ");
                }
                result.append(keyColumn);
            }
        }
        if (includeValueFields) {
            List<String> valueColumns = getValueColumns();
            for (String valueColumn : valueColumns) {
                if (first) {
                    first = false;
                }
                else {
                    result.append(", ");
                }
                result.append(valueColumn);
            }
        }

        result.append(" from ");
        result.append(keyspace);
        result.append(".").append(table);
        if (includeFilter && !keyColumns.isEmpty()) {
            result.append(" where ");
            first = true;
            for (String keyColumn : keyColumns) {
                if (first) {
                    first = false;
                }
                else {
                    result.append(", ");
                }
                result.append(keyColumn).append("=?");
            }
        }
        result.append(";");
        return result.toString();
    }

    /**
     * Binds Ignite cache key object to {@link PreparedStatement}.
     *
     * @param statement statement to which key object should be bind.
     * @param key key object.
     * @return statement with bounded key.
     */
    public BoundStatement bindKey(PreparedStatement statement, Object key) {
        KeyPersistenceSettings settings = persistenceSettings.getKeyPersistenceSettings();
        Object[] values = getBindingValues(settings.getStrategy(),
            settings.getSerializer(), settings.getFields(), key);
        return statement.bind(values);
    }

    /**
     * Binds Ignite cache key and value object to {@link PreparedStatement}.
     *
     * @param statement statement to which key and value object should be bind.
     * @param key key object.
     * @param val value object.
     * @return statement with bounded key and value.
     */
    public BoundStatement bindKeyValue(PreparedStatement statement, Object key, Object val) {
        KeyPersistenceSettings keySettings = persistenceSettings.getKeyPersistenceSettings();
        Object[] keyValues = getBindingValues(keySettings.getStrategy(),
            keySettings.getSerializer(), keySettings.getFields(), key);
        ValuePersistenceSettings valSettings = persistenceSettings.getValuePersistenceSettings();
        Object[] valValues = getBindingValues(valSettings.getStrategy(),
            valSettings.getSerializer(), valSettings.getFields(), val);
        Object[] values = new Object[keyValues.length + valValues.length];
        int i = 0;
        for (Object keyVal : keyValues) {
            values[i] = keyVal;
            i++;
        }
        for (Object valVal : valValues) {
            values[i] = valVal;
            i++;
        }
        return statement.bind(values);
    }

    /**
     * Builds Ignite cache key object from returned Cassandra table row.
     *
     * @param row Cassandra table row.
     * @return key object.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Object buildKeyObject(Row row) {
        return buildObject(row, persistenceSettings.getKeyPersistenceSettings());
    }

    /**
     * Builds Ignite cache value object from Cassandra table row .
     *
     * @param row Cassandra table row.
     * @return value object.
     */
    public Object buildValueObject(Row row) {
        return buildObject(row, persistenceSettings.getValuePersistenceSettings());
    }

    /**
     * Builds object from Cassandra table row.
     *
     * @param row Cassandra table row.
     * @param settings persistence settings to use.
     * @return object.
     */
    protected Object buildObject(Row row, PersistenceSettings settings) {
        if (row == null) {
            return null;
        }
        PersistenceStrategy stgy = settings.getStrategy();
        Class clazz = settings.getJavaClass();
        String col = settings.getColumn();
        List<PojoField> fields = settings.getFields();
        if (PersistenceStrategy.PRIMITIVE.equals(stgy)) {
            return PropertyMappingHelper.getCassandraColumnValue(row, col, clazz, null);
        }
        if (PersistenceStrategy.BLOB.equals(stgy)) {
            return settings.getSerializer().deserialize(row.getBytes(col));
        }
        Object obj;
        try {
            obj = clazz.newInstance();
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to instantiate object of type '" + clazz.getName() + "' using reflection", e);
        }
        for (PojoField field : fields) {
            field.setValueFromRow(row, obj, settings.getSerializer());
        }
        return obj;
    }

    /**
     * Extracts field values from POJO object and converts them into Java types which could be mapped to Cassandra
     * types.
     *
     * @param stgy persistence strategy to use.
     * @param serializer serializer to use for BLOBs.
     * @param fields fields who's values should be extracted.
     * @param obj object instance who's field values should be extracted.
     * @return array of object field values converted into Java object instances having Cassandra compatible types
     */
    private Object[] getBindingValues(PersistenceStrategy stgy, Serializer serializer, List<PojoField> fields,
        Object obj) {
        if (PersistenceStrategy.PRIMITIVE.equals(stgy)) {
            if (PropertyMappingHelper.getCassandraType(obj.getClass()) == null ||
                obj.getClass().equals(ByteBuffer.class) || obj instanceof byte[]) {
                throw new IllegalArgumentException("Couldn't deserialize instance of class '" +
                    obj.getClass().getName() + "' using PRIMITIVE strategy. Please use BLOB strategy for this case.");
            }
            return new Object[] {obj};
        }
        if (PersistenceStrategy.BLOB.equals(stgy)) {
            return new Object[] {serializer.serialize(obj)};
        }
        Object[] values = new Object[fields.size()];
        int i = 0;
        for (PojoField field : fields) {
            Object val = field.getValueFromObject(obj, serializer);
            if (val instanceof byte[]) {
                val = ByteBuffer.wrap((byte[])val);
            }
            values[i] = val;
            i++;
        }
        return values;
    }

    /**
     * Returns list of Cassandra table columns mapped to Ignite cache key and value fields
     *
     * @return list of column names
     */
    private List<String> getKeyValueColumns() {
        List<String> cols = getKeyColumns();
        cols.addAll(getValueColumns());
        return cols;
    }

    /**
     * Returns list of Cassandra table columns mapped to Ignite cache key fields
     *
     * @return list of column names
     */
    private List<String> getKeyColumns() {
        return getColumns(persistenceSettings.getKeyPersistenceSettings());
    }

    /**
     * Returns list of Cassandra table columns mapped to Ignite cache value fields
     *
     * @return list of column names
     */
    private List<String> getValueColumns() {
        return getColumns(persistenceSettings.getValuePersistenceSettings());
    }

    /**
     * Returns list of Cassandra table columns based on persistence strategy to use
     *
     * @return list of column names
     */
    private List<String> getColumns(PersistenceSettings settings) {
        List<String> cols = new LinkedList<>();
        if (!PersistenceStrategy.POJO.equals(settings.getStrategy())) {
            cols.add(settings.getColumn());
            return cols;
        }
        for (PojoField field : settings.getFields()) {
            cols.add(field.getColumn());
        }
        return cols;
    }
}

