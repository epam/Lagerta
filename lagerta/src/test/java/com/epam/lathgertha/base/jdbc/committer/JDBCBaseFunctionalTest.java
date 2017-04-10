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
import com.epam.lathgertha.capturer.JDBCDataCapturerLoader;
import com.epam.lathgertha.resources.DBResource;
import com.epam.lathgertha.util.SerializerImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import javax.sql.rowset.serial.SerialBlob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

public abstract class JDBCBaseFunctionalTest extends BaseFunctionalTest {

    protected static final String SELECT_FROM_TEMPLATE = "SELECT * FROM %s";
    protected static final String INSERT_INTO_TEMPLATE = "INSERT INTO %s VALUES (%s)";
    protected static final String DATA_PROVIDER_VAL_NAME = "val";
    protected static final String DATA_BASE_NAME = "h2_functional_test";
    protected static final String DATA_PROVIDER_NOT_PERSON = "notPersonData";
    protected static final String DATA_PROVIDER_LIST_VAL_NAME = "listValues";

    protected static Connection connection;
    protected static String dbUrl;

    protected static JDBCCommitter jdbcCommitter;
    protected static JDBCDataCapturerLoader jdbcDataCapturerLoader;

    @BeforeClass
    public static void init() throws Exception {
        DBResource dbResource = new DBResource(DATA_BASE_NAME);
        dbUrl = dbResource.getDBUrl();
        connection = dbResource.getConnection();
    }

    @AfterClass
    public static void clean() throws Exception {
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

    @DataProvider(name = DATA_PROVIDER_NOT_PERSON)
    public static Object[][] notPerson() {
        return new Object[][]{
                {1, 2,},
                {2, "string"},
                {3, 'c',},
                {4, new Date(System.currentTimeMillis())},
        };
    }

    @DataProvider(name = DATA_PROVIDER_LIST_VAL_NAME)
    public static Object[][] listVal() {
        return new Object[][]{
                {Arrays.asList(1, 2, 3, 4), Arrays.asList(1, 2, 3, 4), Integer.class},
                {Arrays.asList(1, 2), Arrays.asList("1", "2"), String.class},
                {Arrays.asList(1, 2), Arrays.asList(new Person(1, "Name1"), new Person(2, "Name2")), Person.class}
        };
    }

    protected void insertIntoToPersonTable(Integer key, Object val, String name, Integer id) throws SQLException {
        String maskFields = PersonEntries.getPersonFieldDescriptor().entrySet().stream()
                .map(i -> "?")
                .collect(Collectors.joining(", "));
        PreparedStatement preparedStatement = connection.prepareStatement(String.format(INSERT_INTO_TEMPLATE, Person.PERSON_TABLE, maskFields));
        preparedStatement.setInt(Person.PERSON_KEY_INDEX, key);

        if (id != null) {
            preparedStatement.setInt(Person.PERSON_ID_INDEX, id);
        } else {
            preparedStatement.setObject(Person.PERSON_ID_INDEX, null);
        }
        if (val != null) {
            preparedStatement.setBlob(Person.PERSON_VAL_INDEX, new SerialBlob(new SerializerImpl().serialize(val).array()));
        } else {
            preparedStatement.setObject(Person.PERSON_VAL_INDEX, null);
        }
        if (name != null) {
            preparedStatement.setString(Person.PERSON_NAME_INDEX, name);
        } else {
            preparedStatement.setObject(Person.PERSON_NAME_INDEX, null);
        }
        preparedStatement.execute();
    }

}
