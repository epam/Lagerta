/*
 * Copyright (c) 2017. EPAM Systems.
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

package com.epam.lagerta.base.jdbc.common;

import com.epam.lagerta.base.FieldDescriptor;
import org.apache.ignite.binary.BinaryObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class KeyValueAndMetadata {
    private final int key;
    private final Object value;
    private final String cacheName;
    private final String tableName;
    private final Map<String, Object> keyValueMap;
    private final List<FieldDescriptor> fieldDescriptors;
    private final ResultMapGetter resultMapGetter;

    public KeyValueAndMetadata(
            int key,
            Object value,
            String cacheName,
            String table,
            Map<String, Object> keyValueMap,
            List<FieldDescriptor> fieldDescriptors,
            ResultMapGetter resultMapGetter
    ) {
        this.key = key;
        this.value = value;
        this.cacheName = cacheName;
        this.tableName = table;
        this.keyValueMap = keyValueMap;
        this.fieldDescriptors = fieldDescriptors;
        this.resultMapGetter = resultMapGetter;
    }

    public int getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public Object getUnwrappedValue() {
        return value instanceof BinaryObject ? ((BinaryObject) value).deserialize() : value;
    }

    public String getCacheName() {
        return cacheName;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, Object> getKeyValueMap() {
        return keyValueMap;
    }

    public List<FieldDescriptor> getFieldDescriptors() {
        return fieldDescriptors;
    }

    public ResultMapGetter getResultMapGetter() {
        return resultMapGetter;
    }

    @FunctionalInterface
    public interface ResultMapGetter {
        Map<String, Object> getResultMap(ResultSet resultSet) throws SQLException;
    }
}
