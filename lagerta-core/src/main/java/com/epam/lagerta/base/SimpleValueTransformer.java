/*
 * Copyright 2017 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.lagerta.base;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum SimpleValueTransformer implements ValueTransformer {
    DUMMY(null, null),

    STRING(ResultSet::getString, (Setter<String>) PreparedStatement::setString, String.class),
    BOOLEAN(ResultSet::getBoolean, (Setter<Boolean>) PreparedStatement::setBoolean, Boolean.class, Boolean.TYPE),
    BYTE(ResultSet::getByte, (Setter<Byte>) PreparedStatement::setByte, Byte.class, Byte.TYPE),
    SHORT(ResultSet::getShort, (Setter<Short>) PreparedStatement::setShort, Short.class, Short.TYPE),
    INTEGER(ResultSet::getInt, (Setter<Integer>) PreparedStatement::setInt, Integer.class, Integer.TYPE),
    LONG(ResultSet::getLong, (Setter<Long>) PreparedStatement::setLong, Long.class, Long.TYPE),
    FLOAT(ResultSet::getFloat, (Setter<Float>) PreparedStatement::setFloat, Float.class, Float.TYPE),
    DOUBLE(ResultSet::getDouble, (Setter<Double>) PreparedStatement::setDouble, Double.class, Double.TYPE),
    BIG_DECIMAL(ResultSet::getBigDecimal, (Setter<BigDecimal>) PreparedStatement::setBigDecimal, BigDecimal.class),
    BYTES(ResultSet::getBytes, (Setter<byte[]>) PreparedStatement::setBytes, byte[].class),
    DATE(SimpleValueTransformer::getDate, SimpleValueTransformer::setDate, java.util.Date.class),
    TIMESTAMP(ResultSet::getTimestamp, (Setter<Timestamp>) PreparedStatement::setTimestamp, Timestamp.class);

    private static final Map<Class<?>, ValueTransformer> MATCH = Arrays.stream(values())
            .flatMap(transformer -> Arrays.stream(transformer.matchedTo)
                    .map(clazz -> new AbstractMap.SimpleImmutableEntry<Class<?>, ValueTransformer>(clazz, transformer)))
            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));

    private static final Function<Class, ValueTransformer> FIND = key -> MATCH.entrySet().stream()
            .filter(entry -> entry.getKey().isAssignableFrom(key))
            .findFirst()
            .map(Map.Entry::getValue)
            .orElse(DUMMY);

    private final Getter getter;
    private final Setter setter;
    private final Class<?>[] matchedTo;

    SimpleValueTransformer(Getter getter, Setter setter, Class<?>... matchedTo) {
        this.getter = getter;
        this.setter = setter;
        this.matchedTo = matchedTo;
    }

    @Override
    public Object get(ResultSet resultSet, int index) throws SQLException {
        return getter.get(resultSet, index);
    }

    @Override
    public void set(PreparedStatement preparedStatement, int index, Object value) throws SQLException {
        setter.set(preparedStatement, index, value);
    }

    public static ValueTransformer get(Class<?> clazz) {
        return MATCH.computeIfAbsent(clazz, FIND);
    }

    //------------------------------------------------------------------------------------------------------------------

    private static Object getDate(ResultSet resultSet, int index) throws SQLException {
        Date date = resultSet.getDate(index);
        return date == null ? null : new java.util.Date(date.getTime());
    }

    private static void setDate(PreparedStatement preparedStatement, int index, Object value) throws SQLException {
        preparedStatement.setDate(index, new Date(((java.util.Date) value).getTime()));
    }

    //------------------------------------------------------------------------------------------------------------------

    private interface Getter {
        Object get(ResultSet resultSet, int index) throws SQLException;
    }

    private interface Setter<V> {
        void set(PreparedStatement preparedStatement, int index, V value) throws SQLException;
    }
}
