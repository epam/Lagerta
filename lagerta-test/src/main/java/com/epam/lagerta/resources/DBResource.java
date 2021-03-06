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

package com.epam.lagerta.resources;

import com.epam.lagerta.base.jdbc.H2HikariDataSource;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DBResource implements Resource {
    private final String dbName;

    private HikariDataSource dataSource;

    public DBResource(String dbName) {
        this.dbName = dbName;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public void setUp() {
        dataSource = H2HikariDataSource.create(dbName);
    }

    @Override
    public void tearDown() {
        dataSource.close();
    }

    public void executeResource(String resourceName) throws SQLException {
        URL resource = DBResource.class.getResource(resourceName);
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            String query = Resources.toString(resource, Charsets.UTF_8);
            statement.executeUpdate(query);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
