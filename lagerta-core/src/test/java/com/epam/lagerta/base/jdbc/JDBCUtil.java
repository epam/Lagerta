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

package com.epam.lagerta.base.jdbc;

import com.epam.lagerta.base.BlobValueTransformer;
import com.epam.lagerta.base.FieldDescriptor;
import com.epam.lagerta.base.ValueTransformer;
import com.epam.lagerta.base.jdbc.committer.JDBCCommitter;
import com.epam.lagerta.base.jdbc.committer.SQLSupplier;
import com.epam.lagerta.base.jdbc.common.KeyValueAndMetadata;
import com.epam.lagerta.capturer.JDBCDataCapturerLoader;
import com.epam.lagerta.util.JDBCKeyValueMapper;
import com.epam.lagerta.util.Serializer;
import com.epam.lagerta.util.SerializerImpl;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import javax.sql.DataSource;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.stream.Collectors;

public final class JDBCUtil {
    private static final String SQL_BASE_PATH = "/com/epam/lagerta/base/jdbc/committer/";
    private static final String INSERT_INTO_TEMPLATE = "INSERT INTO %s VALUES (%s)";
    private static final Serializer SERIALIZER = new SerializerImpl();

    public static final ValueTransformer BLOB_TRANSFORMER = new BlobValueTransformer(SERIALIZER);
    public static final String CREATE_TABLE_SQL_RESOURCE = SQL_BASE_PATH + "create_tables.sql";
    public static final String DROP_TABLE_SQL_RESOURCE = SQL_BASE_PATH + "drop_tables.sql";
    public static final String SELECT_FROM_TEMPLATE = "SELECT * FROM %s";

    private JDBCUtil() {
    }

    public static JDBCCommitter getJDBCCommitter(DataSource dataSource) {
        return new JDBCCommitter(dataSource, EntityDescriptors.getEntityDescriptors());
    }

    public static JDBCDataCapturerLoader getJDBCDataCapturerLoader(DataSource dataSource) {
        return new JDBCDataCapturerLoader(dataSource, EntityDescriptors.getEntityDescriptors());
    }

    public static void executeUpdateQueryFromResource(Connection connection, String resourceName) {
        URL resource = JDBCUtil.class.getResource(resourceName);
        try {
            String query = Resources.toString(resource, Charsets.UTF_8);
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(query);
            }
            connection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void applyInConnection(DataSource dataSource, SQLSupplier<Connection> function) {
        try (Connection connection = dataSource.getConnection()) {
            function.apply(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isOrdinaryColumn(String column) {
        return !(JDBCKeyValueMapper.KEY_FIELD_NAME.equals(column)
                || JDBCKeyValueMapper.VAL_FIELD_NAME.equals(column));
    }

    public static void fillSpecialColumnsFromResultSet(ResultSet resultSet, Map<String, Object> keyValueMap) throws SQLException {
        Blob blob = resultSet.getBlob(JDBCKeyValueMapper.VAL_FIELD_NAME);
        Object deserializedVal = null;

        if (blob != null) {
            int length = (int) blob.length();
            deserializedVal = SERIALIZER.deserialize(ByteBuffer.wrap(blob.getBytes(1, length)));
        }
        keyValueMap.put(JDBCKeyValueMapper.VAL_FIELD_NAME, deserializedVal);
        keyValueMap.put(JDBCKeyValueMapper.KEY_FIELD_NAME, resultSet.getInt(JDBCKeyValueMapper.KEY_FIELD_NAME));
    }

    public static void insertIntoDB(DataSource dataSource, KeyValueAndMetadata kvMeta) throws SQLException {
        applyInConnection(dataSource, connection -> insertIntoDB(connection, kvMeta));
    }

    public static void insertIntoDB(Connection connection, KeyValueAndMetadata kvMeta) throws SQLException {
        String maskFields = kvMeta
                .getFieldDescriptors()
                .entrySet()
                .stream()
                .map(i -> "?")
                .collect(Collectors.joining(", "));
        try (PreparedStatement preparedStatement = connection.prepareStatement(
                String.format(INSERT_INTO_TEMPLATE, kvMeta.getTable(), maskFields))) {
            for (FieldDescriptor descriptor : kvMeta.getFieldDescriptors().values()) {
                set(descriptor, preparedStatement, kvMeta.getKeyValueMap().get(descriptor.getName()));
            }
            preparedStatement.execute();
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    private static void set(FieldDescriptor descriptor, PreparedStatement preparedStatement, Object value) throws SQLException {
        if (value == null) {
            preparedStatement.setObject(descriptor.getIndex(), null);
        } else {
            descriptor.getTransformer().set(preparedStatement, descriptor.getIndex(), value);
        }
    }
}
