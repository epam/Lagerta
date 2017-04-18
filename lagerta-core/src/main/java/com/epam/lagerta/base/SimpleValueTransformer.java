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

public enum SimpleValueTransformer implements ValueTransformer {
    STRING(ResultSet::getString, (Setter<String>) PreparedStatement::setString),
    BOOLEAN(ResultSet::getBoolean, (Setter<Boolean>) PreparedStatement::setBoolean),
    BYTE(ResultSet::getByte, (Setter<Byte>) PreparedStatement::setByte),
    SHORT(ResultSet::getShort, (Setter<Short>) PreparedStatement::setShort),
    INTEGER(ResultSet::getInt, (Setter<Integer>) PreparedStatement::setInt),
    LONG(ResultSet::getLong, (Setter<Long>) PreparedStatement::setLong),
    FLOAT(ResultSet::getFloat, (Setter<Float>) PreparedStatement::setFloat),
    DOUBLE(ResultSet::getDouble, (Setter<Double>) PreparedStatement::setDouble),
    BIG_DECIMAL(ResultSet::getBigDecimal, (Setter<BigDecimal>) PreparedStatement::setBigDecimal),
    BYTES(ResultSet::getBytes, (Setter<byte[]>) PreparedStatement::setBytes),
    DATE(SimpleValueTransformer::getDate, SimpleValueTransformer::setDate),
    TIMESTAMP(SimpleValueTransformer::getTimestamp, SimpleValueTransformer::setTimestamp);

    private final Getter get;
    private final Setter set;

    SimpleValueTransformer(Getter get, Setter set) {
        this.get = get;
        this.set = set;
    }

    @Override
    public Object get(ResultSet resultSet, int index) throws SQLException {
        return get.get(resultSet, index);
    }

    @Override
    public void set(PreparedStatement preparedStatement, int index, Object value) throws SQLException {
        set.set(preparedStatement, index, value);
    }

    //------------------------------------------------------------------------------------------------------------------

    private static Object getDate(ResultSet resultSet, int index) throws SQLException {
        Date date = resultSet.getDate(index);
        return date == null ? null : new java.util.Date(date.getTime());
    }

    private static void setDate(PreparedStatement preparedStatement, int index, Object value) throws SQLException {
        preparedStatement.setDate(index, new Date(((java.util.Date) value).getTime()));
    }

    private static Object getTimestamp(ResultSet resultSet, int index) throws SQLException {
        Timestamp timestamp = resultSet.getTimestamp(index);
        return timestamp == null ? null : new java.util.Date(timestamp.getTime());
    }

    private static void setTimestamp(PreparedStatement preparedStatement, int index, Object value) throws SQLException {
        preparedStatement.setTimestamp(index, new Timestamp(((java.util.Date) value).getTime()));
    }

    //------------------------------------------------------------------------------------------------------------------

    private interface Getter {
        Object get(ResultSet resultSet, int index) throws SQLException;
    }

    private interface Setter<V> {
        void set(PreparedStatement preparedStatement, int index, V value) throws SQLException;
    }
}
