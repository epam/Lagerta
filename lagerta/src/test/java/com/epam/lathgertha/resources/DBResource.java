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

import com.epam.lathgertha.base.jdbc.H2HikariDataSource;
import com.epam.lathgertha.base.jdbc.JDBCUtil;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

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

    public void initState(String resourceName) throws SQLException {
        JDBCUtil.executeUpdateQueryFromResource(dataSource.getConnection(), resourceName);
    }

    public void clearState(String resourceName) throws SQLException {
        JDBCUtil.executeUpdateQueryFromResource(dataSource.getConnection(), resourceName);
    }
}
