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

import com.epam.lagerta.base.FieldDescriptor;
import com.epam.lagerta.base.jdbc.committer.SQLSupplier;
import com.epam.lagerta.base.jdbc.common.Person;
import com.epam.lagerta.base.jdbc.common.PersonEntries;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import javax.sql.DataSource;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.stream.Collectors;

public final class JDBCUtil {
    private static final String INSERT_INTO_TEMPLATE = "INSERT INTO %s VALUES (%s)";

    private JDBCUtil() {
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

    public static void insertIntoPersonTable(
            DataSource dataSource,
            Integer key,
            Object val,
            String name,
            Integer id
    ) throws SQLException {
        applyInConnection(dataSource, connection -> insertIntoPersonTable(connection, key, val, name, id));
    }

    public static void insertIntoPersonTable(
            Connection connection,
            Integer key,
            Object val,
            String name,
            Integer id
    ) throws SQLException {
        String maskFields = PersonEntries.getPersonFieldDescriptor().entrySet().stream()
                .map(i -> "?")
                .collect(Collectors.joining(", "));
        try (PreparedStatement preparedStatement = connection.prepareStatement(
                String.format(INSERT_INTO_TEMPLATE, Person.PERSON_TABLE, maskFields))) {
            Map<String, FieldDescriptor> personFieldDescriptor = PersonEntries.getPersonFieldDescriptor();

            personFieldDescriptor.get(Person.PERSON_KEY).setValueInStatement(key, preparedStatement);
            personFieldDescriptor.get(Person.PERSON_VAL).setValueInStatement(val, preparedStatement);
            personFieldDescriptor.get(Person.PERSON_ID).setValueInStatement(id, preparedStatement);
            personFieldDescriptor.get(Person.PERSON_NAME).setValueInStatement(name, preparedStatement);
            preparedStatement.execute();
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }
}
