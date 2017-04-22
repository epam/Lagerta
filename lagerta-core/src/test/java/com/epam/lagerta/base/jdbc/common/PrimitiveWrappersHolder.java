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

import com.epam.lagerta.base.EntityDescriptor;
import com.epam.lagerta.base.FieldDescriptor;
import com.epam.lagerta.base.jdbc.JDBCUtil;
import com.epam.lagerta.util.JDBCKeyValueMapper;
import org.apache.ignite.binary.BinaryObject;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.epam.lagerta.base.SimpleValueTransformer.BOOLEAN;
import static com.epam.lagerta.base.SimpleValueTransformer.BYTE;
import static com.epam.lagerta.base.SimpleValueTransformer.DOUBLE;
import static com.epam.lagerta.base.SimpleValueTransformer.FLOAT;
import static com.epam.lagerta.base.SimpleValueTransformer.INTEGER;
import static com.epam.lagerta.base.SimpleValueTransformer.LONG;
import static com.epam.lagerta.base.SimpleValueTransformer.SHORT;

public class PrimitiveWrappersHolder implements Serializable {
    public static final String CACHE = "primitiveWrappersCache";
    public static final String BINARY_KEEPING_CACHE = "binaryKeepingPrimitiveWrappersCache";
    public static final String TABLE = "primitiveWrappersTable";

    private static final String BOOLEAN_VALUE = "booleanValue";
    private static final String BYTE_VALUE = "byteValue";
    private static final String SHORT_VALUE = "shortValue";
    private static final String INT_VALUE = "intValue";
    private static final String LONG_VALUE = "longValue";
    private static final String FLOAT_VALUE = "floatValue";
    private static final String DOUBLE_VALUE = "doubleValue";

    private static final int KEY_INDEX = 1;
    private static final int VAL_INDEX = 2;
    private static final int BOOLEAN_VALUE_INDEX = 3;
    private static final int BYTE_VALUE_INDEX = 4;
    private static final int SHORT_VALUE_INDEX = 5;
    private static final int INT_VALUE_INDEX = 6;
    private static final int LONG_VALUE_INDEX = 7;
    private static final int FLOAT_VALUE_INDEX = 8;
    private static final int DOUBLE_VALUE_INDEX = 9;

    private static final Map<String, FieldDescriptor> FIELD_DESCRIPTORS = Stream.of(
            new FieldDescriptor(KEY_INDEX, JDBCKeyValueMapper.KEY_FIELD_NAME, INTEGER),
            new FieldDescriptor(VAL_INDEX, JDBCKeyValueMapper.VAL_FIELD_NAME, JDBCUtil.BLOB_TRANSFORMER),
            new FieldDescriptor(BOOLEAN_VALUE_INDEX, BOOLEAN_VALUE, BOOLEAN),
            new FieldDescriptor(BYTE_VALUE_INDEX, BYTE_VALUE, BYTE),
            new FieldDescriptor(SHORT_VALUE_INDEX, SHORT_VALUE, SHORT),
            new FieldDescriptor(INT_VALUE_INDEX, INT_VALUE, INTEGER),
            new FieldDescriptor(LONG_VALUE_INDEX, LONG_VALUE, LONG),
            new FieldDescriptor(FLOAT_VALUE_INDEX, FLOAT_VALUE, FLOAT),
            new FieldDescriptor(DOUBLE_VALUE_INDEX, DOUBLE_VALUE, DOUBLE)
    ).collect(Collectors.toMap(FieldDescriptor::getName, Function.identity()));

    private static final List<String> ORDINARY_COLUMNS = FIELD_DESCRIPTORS
            .keySet()
            .stream()
            .filter(JDBCUtil::isOrdinaryColumn)
            .collect(Collectors.toList());

    public static final EntityDescriptor ENTITY_DESCRIPTOR = new EntityDescriptor<>(
            PrimitiveWrappersHolder.class,
            TABLE,
            JDBCKeyValueMapper.KEY_FIELD_NAME,
            FIELD_DESCRIPTORS
    );

    public static Map<String, Object> getResultMap(ResultSet resultSet) throws SQLException {
        Map<String, Object> keyValueMap = new HashMap<>(FIELD_DESCRIPTORS.size());

        for (String column : ORDINARY_COLUMNS) {
            keyValueMap.put(column, resultSet.getObject(column));
        }
        JDBCUtil.fillSpecialColumnsFromResultSet(resultSet, keyValueMap);
        return keyValueMap;
    }

    public static KeyValueAndMetadata withMetaData(int key, Object value) {
        boolean asBinary = value instanceof BinaryObject;
        PrimitiveWrappersHolder holder = asBinary ? ((BinaryObject) value).deserialize()
                : (PrimitiveWrappersHolder) value;
        Map<String, Object> keyValueMap = toMap(key, holder, asBinary);

        return new KeyValueAndMetadata(
                key,
                value,
                asBinary ? BINARY_KEEPING_CACHE : CACHE,
                TABLE,
                keyValueMap,
                FIELD_DESCRIPTORS,
                PrimitiveWrappersHolder::getResultMap
        );
    }

    public static Map<String, Object> toMap(int key, PrimitiveWrappersHolder holder, boolean asBinary) {
        Map<String, Object> keyValueMap = new HashMap<>(FIELD_DESCRIPTORS.size());

        keyValueMap.put(JDBCKeyValueMapper.KEY_FIELD_NAME, key);
        if (asBinary) {
            keyValueMap.put(BOOLEAN_VALUE, holder.booleanValue);
            keyValueMap.put(BYTE_VALUE, holder.byteValue);
            keyValueMap.put(SHORT_VALUE, holder.shortValue);
            keyValueMap.put(INT_VALUE, holder.intValue);
            keyValueMap.put(LONG_VALUE, holder.longValue);
            keyValueMap.put(FLOAT_VALUE, holder.floatValue);
            keyValueMap.put(DOUBLE_VALUE, holder.doubleValue);
            keyValueMap.put(JDBCKeyValueMapper.VAL_FIELD_NAME, null);
        } else {
            ORDINARY_COLUMNS.forEach(column -> keyValueMap.put(column, null));
            keyValueMap.put(JDBCKeyValueMapper.VAL_FIELD_NAME, holder);
        }
        return keyValueMap;
    }

    private Boolean booleanValue;
    private Byte byteValue;
    private Short shortValue;
    private Integer intValue;
    private Long longValue;
    private Float floatValue;
    private Double doubleValue;

    @SuppressWarnings("unused")
    public PrimitiveWrappersHolder() {
    }

    public PrimitiveWrappersHolder(
            Boolean booleanValue,
            Byte byteValue,
            Short shortValue,
            Integer intValue,
            Long longValue,
            Float floatValue,
            Double doubleValue
    ) {
        this.booleanValue = booleanValue;
        this.byteValue = byteValue;
        this.shortValue = shortValue;
        this.intValue = intValue;
        this.longValue = longValue;
        this.floatValue = floatValue;
        this.doubleValue = doubleValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PrimitiveWrappersHolder)) {
            return false;
        }
        PrimitiveWrappersHolder other = (PrimitiveWrappersHolder) obj;

        return Objects.equals(booleanValue, other.booleanValue) &&
                Objects.equals(byteValue, other.byteValue) &&
                Objects.equals(shortValue, other.shortValue) &&
                Objects.equals(intValue, other.intValue) &&
                Objects.equals(longValue, other.longValue) &&
                Objects.equals(floatValue, other.floatValue) &&
                Objects.equals(doubleValue, other.doubleValue);
    }

    @Override
    public String toString() {
        return "PrimitiveWrappersHolder{" +
                "booleanValue=" + booleanValue +
                ", byteValue=" + byteValue +
                ", shortValue=" + shortValue +
                ", intValue=" + intValue +
                ", longValue=" + longValue +
                ", floatValue=" + floatValue +
                ", doubleValue=" + doubleValue +
                '}';
    }
}
