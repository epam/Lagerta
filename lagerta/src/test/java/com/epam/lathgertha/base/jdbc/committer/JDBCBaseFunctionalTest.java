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

package com.epam.lathgertha.base.jdbc.committer;

import com.epam.lathgertha.BaseFunctionalTest;
import com.epam.lathgertha.base.jdbc.JDBCUtil;
import com.epam.lathgertha.base.jdbc.common.Person;
import com.epam.lathgertha.base.jdbc.common.PersonEntries;
import com.epam.lathgertha.resources.DBResource;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public abstract class JDBCBaseFunctionalTest extends BaseFunctionalTest {

    protected static final String DATA_BASE_NAME = "h2_functional_test";
    protected static final String DATA_PROVIDER_VAL_NAME = "val";

    @DataProvider(name = DATA_PROVIDER_VAL_NAME)
    public static Object[][] primitives() {
        return new Object[][]{
                {1, 2, null, 0},
                {2, "string", null, 0},
                {3, 'c', null, 0},
                {4, new Date(System.currentTimeMillis()), null, 0},
                {5, 0.2, null, 0},
                {6, new Person(1, "Name"), null, 0}
        };
    }

    private Connection connection;
    protected String dbUrl;

    @BeforeClass
    public void init() throws Exception {
        DBResource dbResource = new DBResource(DATA_BASE_NAME);
        dbUrl = dbResource.getDBUrl();
        connection = dbResource.getConnection();
    }

    @AfterClass
    public void clean() throws Exception {
        connection.close();
    }

    @BeforeMethod()
    public void initState() throws SQLException {
        JDBCUtil.executeUpdateQueryFromResource(connection, PersonEntries.CREATE_TABLE_SQL_RESOURCE);
    }

    @AfterMethod
    public void clearBase() throws SQLException {
        JDBCUtil.executeUpdateQueryFromResource(connection, PersonEntries.DROP_TABLE_SQL_RESOUCE);
    }

    protected ResultSet executeQuery(String query) throws SQLException {
        return connection.createStatement().executeQuery(query);
    }

    protected PreparedStatement getPrepareStatement(String query) throws SQLException {
        return connection.prepareStatement(query);
    }
}
