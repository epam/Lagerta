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
import com.epam.lagerta.base.util.FieldDescriptorHelper;
import org.apache.ignite.binary.BinaryObject;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.epam.lagerta.base.SimpleValueTransformer.BIG_DECIMAL;
import static com.epam.lagerta.base.SimpleValueTransformer.BYTES;
import static com.epam.lagerta.base.SimpleValueTransformer.DATE;
import static com.epam.lagerta.base.SimpleValueTransformer.INTEGER;
import static com.epam.lagerta.base.SimpleValueTransformer.TIMESTAMP;
import static com.epam.lagerta.base.jdbc.JDBCUtil.SERIALIZER;
import static com.epam.lagerta.util.DataProviderUtil.list;

public class OtherTypesHolder implements Serializable {
    public static final String CACHE = "otherTypesCache";
    public static final String BINARY_KEEPING_CACHE = "binaryKeepingOtherTypesCache";
    public static final String TABLE = "otherTypesTable";

    private static final String BYTES_VALUE = "bytesValue";
    private static final String BIG_DECIMAL_VALUE = "bigDecimalValue";
    private static final String DATE_VALUE = "dateValue";
    private static final String TIMESTAMP_VALUE = "timestampValue";

    private static final int KEY_INDEX = 1;
    private static final int VAL_INDEX = 2;
    private static final int BYTES_VALUE_INDEX = 3;
    private static final int BIG_DECIMAL_VALUE_INDEX = 4;
    private static final int DATE_VALUE_INDEX = 5;
    private static final int TIMESTAMP_VALUE_INDEX = 6;

    private static final List<FieldDescriptor> FIELD_DESCRIPTORS = list(
            new FieldDescriptor(KEY_INDEX, EntityDescriptor.KEY_FIELD_NAME, INTEGER),
            new FieldDescriptor(VAL_INDEX, EntityDescriptor.VAL_FIELD_NAME, JDBCUtil.BLOB_TRANSFORMER),
            new FieldDescriptor(BYTES_VALUE_INDEX, BYTES_VALUE, BYTES),
            new FieldDescriptor(BIG_DECIMAL_VALUE_INDEX, BIG_DECIMAL_VALUE, BIG_DECIMAL),
            new FieldDescriptor(DATE_VALUE_INDEX, DATE_VALUE, DATE),
            new FieldDescriptor(TIMESTAMP_VALUE_INDEX, TIMESTAMP_VALUE, TIMESTAMP));

    private static final List<String> ORDINARY_COLUMNS = FIELD_DESCRIPTORS
            .stream()
            .map(FieldDescriptor::getName)
            .filter(JDBCUtil::isOrdinaryColumn)
            .collect(Collectors.toList());

    public static final EntityDescriptor ENTITY_DESCRIPTOR = new EntityDescriptor<>(
            OtherTypesHolder.class,
            TABLE,
            FIELD_DESCRIPTORS,
            new FieldDescriptorHelper(SERIALIZER)
    );

    private byte[] bytesValue;
    private BigDecimal bigDecimalValue;
    private Date dateValue;
    private Timestamp timestampValue;

    @SuppressWarnings("unused")
    public OtherTypesHolder() {
    }

    public OtherTypesHolder(
            byte[] bytesValue,
            BigDecimal bigDecimalValue,
            Date dateValue,
            Timestamp timestampValue
    ) {
        this.bytesValue = bytesValue;
        this.bigDecimalValue = bigDecimalValue;
        this.dateValue = dateValue;
        this.timestampValue = timestampValue;
    }

    public static Map<String, Object> getResultMap(ResultSet resultSet) throws SQLException {
        Map<String, Object> keyValueMap = new HashMap<>(FIELD_DESCRIPTORS.size());

        JDBCUtil.fillSpecialColumnsFromResultSet(resultSet, keyValueMap);
        keyValueMap.put(BYTES_VALUE, resultSet.getBytes(BYTES_VALUE));
        keyValueMap.put(BIG_DECIMAL_VALUE, resultSet.getBigDecimal(BIG_DECIMAL_VALUE));
        Date date = resultSet.getDate(DATE_VALUE);

        if (date != null) {
            date = new Date(date.getTime());
        }
        keyValueMap.put(DATE_VALUE, date);
        keyValueMap.put(TIMESTAMP_VALUE, resultSet.getTimestamp(TIMESTAMP_VALUE));
        return keyValueMap;
    }

    public static KeyValueAndMetadata withMetaData(int key, Object value) {
        boolean asBinary = value instanceof BinaryObject;
        OtherTypesHolder holder = asBinary ? ((BinaryObject) value).deserialize() : (OtherTypesHolder) value;
        Map<String, Object> keyValueMap = toMap(key, holder, asBinary);

        return new KeyValueAndMetadata(
                key,
                value,
                asBinary ? BINARY_KEEPING_CACHE : CACHE,
                TABLE,
                keyValueMap,
                FIELD_DESCRIPTORS,
                OtherTypesHolder::getResultMap
        );
    }

    public static Map<String, Object> toMap(int key, OtherTypesHolder holder, boolean asBinary) {
        Map<String, Object> keyValueMap = new HashMap<>(FIELD_DESCRIPTORS.size());

        keyValueMap.put(EntityDescriptor.KEY_FIELD_NAME, key);
        if (asBinary) {
            keyValueMap.put(BYTES_VALUE, holder.bytesValue);
            keyValueMap.put(BIG_DECIMAL_VALUE, holder.bigDecimalValue);
            keyValueMap.put(DATE_VALUE, holder.dateValue);
            keyValueMap.put(TIMESTAMP_VALUE, holder.timestampValue);
            keyValueMap.put(EntityDescriptor.VAL_FIELD_NAME, null);
        } else {
            ORDINARY_COLUMNS.forEach(column -> keyValueMap.put(column, null));
            keyValueMap.put(EntityDescriptor.VAL_FIELD_NAME, holder);
        }
        return keyValueMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OtherTypesHolder)) return false;
        OtherTypesHolder that = (OtherTypesHolder) o;
        return Arrays.equals(bytesValue, that.bytesValue) &&
                Objects.equals(bigDecimalValue, that.bigDecimalValue) &&
                Objects.equals(dateValue, that.dateValue) &&
                Objects.equals(timestampValue, that.timestampValue);
    }

    @Override
    public String toString() {
        return "OtherTypesHolder{" +
                "bytesValue=" + Arrays.toString(bytesValue) +
                ", bigDecimalValue=" + bigDecimalValue +
                ", dateValue=" + dateValue +
                ", timestampValue=" + timestampValue +
                '}';
    }
}
