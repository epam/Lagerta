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
import com.epam.lagerta.base.jdbc.JDBCUtil;
import org.apache.ignite.binary.BinaryObject;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.epam.lagerta.base.jdbc.common.PrimitivesFieldConstants.BOOLEAN_VALUE;
import static com.epam.lagerta.base.jdbc.common.PrimitivesFieldConstants.BYTE_VALUE;
import static com.epam.lagerta.base.jdbc.common.PrimitivesFieldConstants.DOUBLE_VALUE;
import static com.epam.lagerta.base.jdbc.common.PrimitivesFieldConstants.FIELD_DESCRIPTORS;
import static com.epam.lagerta.base.jdbc.common.PrimitivesFieldConstants.FLOAT_VALUE;
import static com.epam.lagerta.base.jdbc.common.PrimitivesFieldConstants.INT_VALUE;
import static com.epam.lagerta.base.jdbc.common.PrimitivesFieldConstants.LONG_VALUE;
import static com.epam.lagerta.base.jdbc.common.PrimitivesFieldConstants.SHORT_VALUE;

public class PrimitivesHolder implements Serializable {
    public static final String CACHE = "primitivesCache";
    public static final String BINARY_KEEPING_CACHE = "binaryKeepingPrimitivesCache";
    public static final String TABLE = "primitivesTable";

    public static final EntityDescriptor ENTITY_DESCRIPTOR = new EntityDescriptor<>(
            PrimitivesHolder.class,
            TABLE,
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

        keyValueMap.put(EntityDescriptor.KEY_FIELD_NAME, key);
        if (asBinary) {
            keyValueMap.put(BOOLEAN_VALUE, holder.booleanValue);
            keyValueMap.put(BYTE_VALUE, holder.byteValue);
            keyValueMap.put(SHORT_VALUE, holder.shortValue);
            keyValueMap.put(INT_VALUE, holder.intValue);
            keyValueMap.put(LONG_VALUE, holder.longValue);
            keyValueMap.put(FLOAT_VALUE, holder.floatValue);
            keyValueMap.put(DOUBLE_VALUE, holder.doubleValue);
            keyValueMap.put(EntityDescriptor.VAL_FIELD_NAME, null);
        } else {
            // Primitives cannot be loaded as nulls.
            keyValueMap.put(BOOLEAN_VALUE, false);
            keyValueMap.put(BYTE_VALUE, (byte) 0);
            keyValueMap.put(SHORT_VALUE, (short) 0);
            keyValueMap.put(INT_VALUE, 0);
            keyValueMap.put(LONG_VALUE, 0L);
            keyValueMap.put(FLOAT_VALUE, 0F);
            keyValueMap.put(DOUBLE_VALUE, 0D);
            keyValueMap.put(EntityDescriptor.VAL_FIELD_NAME, holder);
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
