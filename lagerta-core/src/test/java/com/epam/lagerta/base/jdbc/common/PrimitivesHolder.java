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

public class PrimitivesHolder implements Serializable {
    public static final String CACHE = "primitivesCache";
    public static final String BINARY_KEEPING_CACHE = "binaryKeepingPrimitivesCache";
    public static final String TABLE = "primitivesTable";

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

    public static final EntityDescriptor ENTITY_DESCRIPTOR = new EntityDescriptor<>(
            PrimitivesHolder.class,
            TABLE,
            JDBCKeyValueMapper.KEY_FIELD_NAME,
            FIELD_DESCRIPTORS
    );

    private boolean booleanValue;
    private byte byteValue;
    private short shortValue;
    private int intValue;
    private long longValue;
    private float floatValue;
    private double doubleValue;

    @SuppressWarnings("unused")
    public PrimitivesHolder() {
    }

    public PrimitivesHolder(
            boolean booleanValue,
            byte byteValue,
            short shortValue,
            int intValue,
            long longValue,
            float floatValue,
            double doubleValue
    ) {
        this.booleanValue = booleanValue;
        this.byteValue = byteValue;
        this.shortValue = shortValue;
        this.intValue = intValue;
        this.longValue = longValue;
        this.floatValue = floatValue;
        this.doubleValue = doubleValue;
    }

    public static Map<String, Object> getResultMap(ResultSet resultSet) throws SQLException {
        Map<String, Object> keyValueMap = new HashMap<>(FIELD_DESCRIPTORS.size());

        JDBCUtil.fillSpecialColumnsFromResultSet(resultSet, keyValueMap);
        keyValueMap.put(BOOLEAN_VALUE, resultSet.getBoolean(BOOLEAN_VALUE));
        keyValueMap.put(BYTE_VALUE, resultSet.getByte(BYTE_VALUE));
        keyValueMap.put(SHORT_VALUE, resultSet.getShort(SHORT_VALUE));
        keyValueMap.put(INT_VALUE, resultSet.getInt(INT_VALUE));
        keyValueMap.put(LONG_VALUE, resultSet.getLong(LONG_VALUE));
        keyValueMap.put(FLOAT_VALUE, resultSet.getFloat(FLOAT_VALUE));
        keyValueMap.put(DOUBLE_VALUE, resultSet.getDouble(DOUBLE_VALUE));
        return keyValueMap;
    }

    public static KeyValueAndMetadata withMetaData(int key, Object value) {
        boolean asBinary = value instanceof BinaryObject;
        PrimitivesHolder holder = asBinary ? ((BinaryObject) value).deserialize() : (PrimitivesHolder) value;
        Map<String, Object> keyValueMap = toMap(key, holder, asBinary);

        return new KeyValueAndMetadata(
                key,
                value,
                asBinary ? BINARY_KEEPING_CACHE : CACHE,
                TABLE,
                keyValueMap,
                FIELD_DESCRIPTORS,
                PrimitivesHolder::getResultMap
        );
    }

    public static Map<String, Object> toMap(int key, PrimitivesHolder holder, boolean asBinary) {
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
            // Primitives cannot be loaded as nulls.
            keyValueMap.put(BOOLEAN_VALUE, false);
            keyValueMap.put(BYTE_VALUE, (byte) 0);
            keyValueMap.put(SHORT_VALUE, (short) 0);
            keyValueMap.put(INT_VALUE, 0);
            keyValueMap.put(LONG_VALUE, 0L);
            keyValueMap.put(FLOAT_VALUE, 0F);
            keyValueMap.put(DOUBLE_VALUE, 0D);
            keyValueMap.put(JDBCKeyValueMapper.VAL_FIELD_NAME, holder);
        }
        return keyValueMap;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PrimitivesHolder)) {
            return false;
        }
        PrimitivesHolder other = (PrimitivesHolder) obj;

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
        return "PrimitivesHolder{" +
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
