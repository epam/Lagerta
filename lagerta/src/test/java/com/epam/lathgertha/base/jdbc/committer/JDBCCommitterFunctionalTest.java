/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lathgertha.base.jdbc.committer;

import com.epam.lathgertha.IgniteConfigurer;
import com.epam.lathgertha.base.jdbc.JDBCUtil;
import com.epam.lathgertha.base.jdbc.common.Person;
import com.epam.lathgertha.base.jdbc.common.PersonEntries;
import com.epam.lathgertha.cluster.SimpleOneProcessClusterManager;
import com.epam.lathgertha.resources.IgniteClusterResource;
import com.epam.lathgertha.util.AtomicsHelper;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCCommitterFunctionalTest {

    private static final String SELECT_FROM_TEMPLATE = "SELECT * FROM %s";
    private static final String DATA_PROVIDER_PRIMITIVES_NAME = "primitives";

    private static File testFolder;
    private static Connection connection;
    private static String dbUrl;

    private static JDBCCommitter jdbcCommitter;

    private static IgniteClusterResource clusterResource = new IgniteClusterResource(1, new SimpleOneProcessClusterManager(prepareIgniteConfig()));

    private static IgniteConfiguration prepareIgniteConfig() {
        IgniteConfiguration result = IgniteConfigurer.getIgniteConfiguration("default");
        result.setCacheConfiguration(new CacheConfiguration(Person.PERSON_CACHE), AtomicsHelper.getConfig());
        return result;
    }

    @BeforeSuite
    public void initCluster() {
        clusterResource.setUp();
    }

    @AfterSuite
    public void stopCluster() {
        clusterResource.tearDown();
    }

    @BeforeClass
    public static void init() throws Exception {
        Class.forName("org.h2.Driver");
        testFolder = createTempDir();
        File tmpFile = File.createTempFile("h2_functional", "test", testFolder);
        dbUrl = "jdbc:h2:file:" + tmpFile.getAbsolutePath();
        connection = DriverManager.getConnection(dbUrl, "", "");
    }

    @AfterClass
    public static void clean() throws Exception {
        connection.close();
        deleteFolder(testFolder);
    }

    @BeforeMethod()
    public void initState() {
        JDBCUtil.executeUpdateQueryFromResource(connection, PersonEntries.CREATE_TABLE_SQL_RESOURCE);
    }

    @AfterMethod
    public void clearBase() {
        JDBCUtil.executeUpdateQueryFromResource(connection, PersonEntries.DROP_TABLE_SQL_RESOUCE);
    }

    private static File createTempDir() {
        File baseDir = new File("."); //in root project
        String baseName = "h2_functional_test" + System.currentTimeMillis();
        File tempDir = new File(baseDir, baseName);
        if (tempDir.mkdir()) {
            return tempDir;
        }
        throw new IllegalStateException("Failed to create directory within " + baseName);
    }

    private static void deleteFolder(File folder) throws IOException {
        Files.walk(Paths.get(folder.getPath()))
                .map(Path::toFile)
                .forEach(File::delete);
        folder.delete();
    }

    @DataProvider(name = DATA_PROVIDER_PRIMITIVES_NAME)
    public static Object[][] primitives() {
        return new Object[][]{
                {1, 2, null, 0},
                {2, "string", null, 0},
                {3, 'c', null, 0},
                {4, new Date(System.currentTimeMillis()), null, 0},
                {5, 0.2, null, 0}
        };
    }

    @Test(dataProvider = DATA_PROVIDER_PRIMITIVES_NAME)
    public void valPrimitivesCommitted(Integer key, Object val, String personName, Integer personId) throws Exception {
        jdbcCommitter = PersonEntries.getPersonOnlyJDBCCommitter(dbUrl);
        Map<String, Object> expectedResult = new HashMap<>(PersonEntries.getPersonColumns().size());
        expectedResult.put(Person.PERSON_KEY, key);
        expectedResult.put(Person.PERSON_VAL, val);
        expectedResult.put(Person.PERSON_NAME, personName);
        expectedResult.put(Person.PERSON_ID, personId);

        jdbcCommitter.commit(Collections.singletonList(Person.PERSON_CACHE).iterator(),
                Collections.<List>singletonList(Collections.singletonList(key)).iterator(),
                Collections.<List<?>>singletonList(Collections.singletonList(val)).iterator());

        String queryForCheckRate = String.format(SELECT_FROM_TEMPLATE, Person.PERSON_TABLE);
        ResultSet resultSet = connection.createStatement().executeQuery(queryForCheckRate);
        Assert.assertTrue(resultSet.next(), "Return empty result");
        Assert.assertEquals(PersonEntries.getResultMapForPerson(resultSet), expectedResult, "Return incorrect result");
    }

    @Test
    public void binaryObjectEntityCommitted() throws Exception {
        jdbcCommitter = PersonEntries.getPersonOnlyJDBCCommitter(dbUrl);
        int key = 22;
        Person expectedPerson = new Person(2, "Name2");
        Ignite ignite = clusterResource.ignite();

        BinaryObject expectedBinary = ignite.binary().toBinary(expectedPerson);

        Map<String, Object> expectedResult = new HashMap<>(PersonEntries.getPersonColumns().size());
        expectedResult.put(Person.PERSON_KEY, key);
        expectedResult.put(Person.PERSON_VAL, null);
        expectedResult.put(Person.PERSON_NAME, expectedPerson.getName());
        expectedResult.put(Person.PERSON_ID, expectedPerson.getId());

        jdbcCommitter.commit(Collections.singletonList(Person.PERSON_CACHE).iterator(),
                Collections.<List>singletonList(Collections.singletonList(key)).iterator(),
                Collections.<List<?>>singletonList(Collections.singletonList(expectedBinary)).iterator());

        String queryForCheckRate = String.format(SELECT_FROM_TEMPLATE, Person.PERSON_TABLE);
        ResultSet resultSet = connection.createStatement().executeQuery(queryForCheckRate);

        Assert.assertTrue(resultSet.next(), "Return empty result");
        Assert.assertEquals(PersonEntries.getResultMapForPerson(resultSet), expectedResult, "Return incorrect result");
    }

    @Test
    public void binaryObjectAndValEntriesCommitted() throws Exception {
        jdbcCommitter = PersonEntries.getPersonOnlyJDBCCommitter(dbUrl);
        int keyVal = 22;
        int val = 10;
        int keyPerson = 23;
        Person expectedPerson = new Person(2, "Name2");
        Ignite ignite = clusterResource.ignite();
        BinaryObject expectedBinary = ignite.binary().toBinary(expectedPerson);

        Map<String, Object> expectedResultForVal = new HashMap<>(PersonEntries.getPersonColumns().size());
        expectedResultForVal.put(Person.PERSON_KEY, keyVal);
        expectedResultForVal.put(Person.PERSON_VAL, val);
        expectedResultForVal.put(Person.PERSON_NAME, null);
        expectedResultForVal.put(Person.PERSON_ID, 0);

        Map<String, Object> expectedResultForPerson = new HashMap<>(PersonEntries.getPersonColumns().size());
        expectedResultForPerson.put(Person.PERSON_KEY, keyPerson);
        expectedResultForPerson.put(Person.PERSON_VAL, null);
        expectedResultForPerson.put(Person.PERSON_NAME, expectedPerson.getName());
        expectedResultForPerson.put(Person.PERSON_ID, expectedPerson.getId());

        int expectedCountRows = 2;

        jdbcCommitter.commit(Collections.singletonList(Person.PERSON_CACHE).iterator(),
                Collections.<List>singletonList(Arrays.asList(keyVal, keyPerson)).iterator(),
                Collections.<List<?>>singletonList(Arrays.asList(val, expectedBinary)).iterator());

        String queryForCheckRate = String.format(SELECT_FROM_TEMPLATE + " ORDER BY KEY ASC", Person.PERSON_TABLE);
        ResultSet resultSet = connection.createStatement().executeQuery(queryForCheckRate);
        List<Map<String, Object>> resultList = new ArrayList<>(expectedCountRows);
        while (resultSet.next()) {
            resultList.add(PersonEntries.getResultMapForPerson(resultSet));
        }
        Assert.assertEquals(resultList.size(), expectedCountRows);
        Assert.assertEquals(resultList.get(0), expectedResultForVal, "Return incorrect result");
        Assert.assertEquals(resultList.get(1), expectedResultForPerson, "Return incorrect result");
    }
}
