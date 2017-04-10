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

package com.epam.lathgertha.base;

import com.epam.lathgertha.util.JDBCKeyValueMapper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EntityDescriptor<T> {
    private final Class<T> clazz;
    private final Map<String, FieldDescriptor> fieldDescriptors;
    private final String upsertQuery;
    private final String selectQuery;

    public EntityDescriptor(Class<T> clazz, Map<String, FieldDescriptor> fieldDescriptors, String tableName, String keyField) {
        this.clazz = clazz;
        this.fieldDescriptors = fieldDescriptors;
        List<String> sortedColumns = fieldDescriptors.entrySet()
                .stream()
                .sorted((e1, e2) -> Integer.valueOf(e1.getValue().getIndex()).compareTo(e2.getValue().getIndex()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        String columnNames = String.join(",", sortedColumns);
        String maskFields = fieldDescriptors.entrySet().stream()
                .map(i -> "?")
                .collect(Collectors.joining(", "));
        //todo need customization sql syntax
        upsertQuery = "MERGE INTO " + tableName + " (" + columnNames + ") KEY(" + keyField + ")" +
                " VALUES (" + maskFields + ")";
        selectQuery = "SELECT " + columnNames + " FROM " + tableName + " WHERE " + keyField + " = ?";
    }

    public String getUpsertQuery() {
        return upsertQuery;
    }

    public String getSelectQuery() {
        return selectQuery;
    }


    public void addValuesToBatch(Object key, Object value, PreparedStatement statement) throws SQLException {
        Map<String, Object> parametersValue = JDBCKeyValueMapper.keyValueMap(key, value);
        for (String fieldName : this.fieldDescriptors.keySet()) {
            Object valueForField = parametersValue.get(fieldName);
            FieldDescriptor fieldDescriptor = this.fieldDescriptors.get(fieldName);
            fieldDescriptor.setValueInStatement(valueForField, statement);
        }
        statement.addBatch();
    }

    public T transform(ResultSet resultSet) throws Exception {
        if (!resultSet.isBeforeFirst()) {
            // empty resultSet
            return null;
        }
        resultSet.next();
        if (!resultSet.isLast()) {
            throw new RuntimeException("Result should be only one row for key");
        }
        Map<String, Object> resultMap = new HashMap<>(fieldDescriptors.size());
        for (Map.Entry<String, FieldDescriptor> descriptorEntry : fieldDescriptors.entrySet()) {
            resultMap.put(descriptorEntry.getKey(), descriptorEntry.getValue().getFieldValue(resultSet));
        }
        return JDBCKeyValueMapper.getObject(resultMap, clazz);
    }
}
