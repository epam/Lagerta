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

package com.epam.lathgertha.resources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBResource implements Resource {
    private static final String CONNECTION_STR_PATTERN = "jdbc:h2:mem:%s";

    private final String dbName;

    // Connection to be hold while resource is active to avoid dropping db
    // because all connections were closed.
    private Connection connection;

    public DBResource(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public void setUp() throws SQLException {
        connection = getConnection();
    }

    @Override
    public void tearDown() throws SQLException {
        connection.close();
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(String.format(CONNECTION_STR_PATTERN, dbName));
    }
}
