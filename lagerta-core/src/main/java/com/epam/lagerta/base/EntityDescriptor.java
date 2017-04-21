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

package com.epam.lagerta.base;

import com.epam.lagerta.base.util.FieldDescriptorHelper;
import com.epam.lagerta.util.JDBCKeyValueMapper;
import com.epam.lagerta.util.SerializerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class EntityDescriptor<T> {
    private static final Logger LOG = LoggerFactory.getLogger(EntityDescriptor.class);

    // todo mark this names as restriction for users
    public static final String KEY_FIELD_NAME = "key";
    public static final String VAL_FIELD_NAME = "val";

    private static final SerializerImpl SERIALIZER = new SerializerImpl();

    private final Class<T> clazz;
    private final String tableName;
    private final List<FieldDescriptor> fieldDescriptors;
    private final String upsertQuery;
    private final String selectQuery;

    public EntityDescriptor(Class<T> clazz, String tableName) {
        this(clazz, tableName, parseClass(clazz));
    }

    public EntityDescriptor(Class<T> clazz, String tableName, List<FieldDescriptor> fieldDescriptors) {
        Objects.requireNonNull(clazz, "class in " + EntityDescriptor.class + " was not set");

        this.clazz = clazz;
        this.tableName = tableName;
        this.fieldDescriptors = setDescriptors(fieldDescriptors);

        List<String> sortedColumns = this.fieldDescriptors
                .stream()
                .sorted((e1, e2) -> Integer.valueOf(e1.getIndex()).compareTo(e2.getIndex()))
                .map(FieldDescriptor::getName)
                .collect(Collectors.toList());

        String columnNames = String.join(",", sortedColumns);
        String maskFields = this.fieldDescriptors.stream()
                .map(i -> "?")
                .collect(Collectors.joining(", "));
        //maybe customization sql syntax for different dialect in future
        upsertQuery = "MERGE INTO " + tableName + " (" + columnNames + ") KEY(" + KEY_FIELD_NAME + ")" +
                " VALUES (" + maskFields + ")";
        // specific IN semantic for h2
        selectQuery = "SELECT " + columnNames + " FROM " + tableName + " WHERE array_contains(?, " + KEY_FIELD_NAME + ")";
    }

    private static List<FieldDescriptor> setDescriptors(List<FieldDescriptor> fieldDescriptors) {
        return new FieldDescriptorHelper(SERIALIZER)
                .addDefaultDescriptors(fieldDescriptors);
    }

    private static <T> List<FieldDescriptor> parseClass(Class<T> clazz) {
        Objects.requireNonNull(clazz, "class in " + EntityDescriptor.class + " was not set");
        LOG.warn("field descriptor was not set, default will be used instead");
        return new FieldDescriptorHelper(SERIALIZER)
                .parseFields(clazz);
    }

    public String getTableName() {
        return tableName;
    }

    public String getUpsertQuery() {
        return upsertQuery;
    }

    public String getSelectQuery() {
        return selectQuery;
    }

    public void addValuesToBatch(Object key, Object value, PreparedStatement statement) throws SQLException {
        Map<String, Object> parametersValue = JDBCKeyValueMapper.keyValueMap(key, value);
        for (FieldDescriptor descriptor : fieldDescriptors) {
            Object valueForField = parametersValue.get(descriptor.getName());
            if (valueForField == null) {
                statement.setObject(descriptor.getIndex(), null);
            } else {
                descriptor.getTransformer().set(statement, descriptor.getIndex(), valueForField);
            }
        }
        statement.addBatch();
    }

    @SuppressWarnings("unchecked")
    public <K> Map<K, T> transform(ResultSet resultSet) throws Exception {
        Map<K, T> result = new HashMap<>();
        while (resultSet.next()) {
            Map<String, Object> objectParameters = new HashMap<>(fieldDescriptors.size());
            for (FieldDescriptor descriptor : fieldDescriptors) {
                Object value = descriptor.getTransformer().get(resultSet, descriptor.getIndex());
                objectParameters.put(descriptor.getName(), value);
            }
            JDBCKeyValueMapper.KeyAndValue<T> keyAndValue = JDBCKeyValueMapper.getObject(objectParameters, clazz);
            result.put((K) keyAndValue.getKey(), keyAndValue.getValue());
        }
        return result;
    }

    @Override
    public String toString() {
        return "Entity {" + ", table '" + tableName + "'\', fields " + fieldDescriptors + '}';
    }
}
