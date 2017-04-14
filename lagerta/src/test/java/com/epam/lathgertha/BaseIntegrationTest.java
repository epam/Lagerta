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

package com.epam.lathgertha;

import com.epam.lathgertha.base.EntityDescriptor;
import com.epam.lathgertha.base.jdbc.JDBCUtil;
import com.epam.lathgertha.base.jdbc.committer.JDBCCommitter;
import com.epam.lathgertha.base.jdbc.common.Person;
import com.epam.lathgertha.base.jdbc.common.PersonEntries;
import com.epam.lathgertha.capturer.DataCapturerLoader;
import com.epam.lathgertha.capturer.JDBCDataCapturerLoader;
import com.epam.lathgertha.resources.DBResource;
import com.epam.lathgertha.resources.FullClusterResource;
import com.epam.lathgertha.subscriber.Committer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseIntegrationTest {
    public static final String CACHE_NAME = "cache";
    public static final String BINARY_KEEPING_CACHE_NAME = "binaryKeepingCache";
    public static final String CACHE_NAME_PROVIDER = "cacheNameProvider";

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseIntegrationTest.class);
    private static final String DB_NAME = "testDB";
    private static final String PERSON_TABLE_SELECT = String.format(
            "SELECT * FROM %s ORDER BY %s ASC",
            Person.PERSON_TABLE,
            Person.PERSON_KEY
    );
    private static final long TX_WAIT_TIME = 10_000;

    private static final Map<String, EntityDescriptor> ENTITY_DESCRIPTOR_MAP = new HashMap<>();

    static {
        ENTITY_DESCRIPTOR_MAP.put(BaseIntegrationTest.CACHE_NAME, PersonEntries.getPersonEntityDescriptor());
        ENTITY_DESCRIPTOR_MAP.put(BaseIntegrationTest.BINARY_KEEPING_CACHE_NAME, PersonEntries.getPersonEntityDescriptor());
    }

    private static int TEST_NUMBER = 0;

    private final FullClusterResource allResources = new FullClusterResource(DB_NAME);

    @DataProvider(name = CACHE_NAME_PROVIDER)
    public static Object[][] provideCacheName() {
        return new Object[][] {
                {CACHE_NAME},
                {BINARY_KEEPING_CACHE_NAME}
        };
    }

    private static String getDBUrl() {
        return String.format(DBResource.CONNECTION_STR_PATTERN, DB_NAME);
    }

    private static Committer personJDBCCommitter() {
        return new JDBCCommitter(ENTITY_DESCRIPTOR_MAP, getDBUrl(), "", "");
    }

    private static DataCapturerLoader personJDBCDataCapturerLoader() {
        return new JDBCDataCapturerLoader(getJdbcDataSource(getDBUrl()), ENTITY_DESCRIPTOR_MAP);
    }

    static DataSource getJdbcDataSource(String url) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl(url);
        dataSource.setUser("");
        dataSource.setPassword("");
        JdbcConnectionPool jdbcConnectionPool = JdbcConnectionPool.create(dataSource);
        jdbcConnectionPool.setMaxConnections(5);
        return jdbcConnectionPool;
    }

    public static String adjustTopicNameForTest(String topic) {
        return topic + "_" + TEST_NUMBER;
    }

    @BeforeSuite
    public void setUp() throws Exception {
        allResources.setUp();
        createDBTable();
    }

    @AfterSuite(alwaysRun = true)
    public void tearDown() {
        allResources.tearDown();
    }

    @AfterMethod
    public void cleanupResources() throws SQLException {
        TEST_NUMBER++;
        allResources.cleanUpClusters();
        createDBTable();
    }

    private void createDBTable() throws SQLException {
        try (Connection connection = allResources.getDBResource().getConnection()) {
            JDBCUtil.executeUpdateQueryFromResource(connection, PersonEntries.CREATE_TABLE_SQL_RESOURCE);
        }
    }

    public Ignite ignite() {
        return allResources.igniteCluster().ignite();
    }

    public void writePersonToCache(String cacheName, int key, Person person) {
        writePersonToCache(ignite(), cacheName, key, person);
    }

    public void writePersonToCache(Ignite ignite, String cacheName, int key, Person person) {
        IgniteCache<Integer, Person> cache = ignite.cache(cacheName);

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(key, person);
            tx.commit();
        }
    }

    public void awaitTransactions() throws InterruptedException {
        Thread.sleep(TX_WAIT_TIME);
        LOGGER.debug("[T] SLEPT {}", TX_WAIT_TIME);
    }

    @SafeVarargs
    public final void assertObjectsInDB(boolean asBinary, Map.Entry<Integer, Person>... persons) throws SQLException {
        try (Connection connection = allResources.getDBResource().getConnection()) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery(PERSON_TABLE_SELECT);

                for (Map.Entry<Integer, Person> entry : persons) {
                    AssertJUnit.assertTrue(resultSet.next());

                    Map<String, Object> expectedMap = personEntryToMap(asBinary, entry);
                    Map<String, Object> actualMap = PersonEntries.getResultMapForPerson(resultSet);

                    AssertJUnit.assertEquals(expectedMap, actualMap);
                }
            }
        }
    }

    private static Map<String, Object> personEntryToMap(boolean asBinary, Map.Entry<Integer, Person> entry) {
        Map<String, Object> result = new HashMap<>(4);
        Person person = entry.getValue();

        result.put(Person.PERSON_KEY, entry.getKey());
        result.put(Person.PERSON_ID, asBinary ? person.getId() : 0);
        result.put(Person.PERSON_NAME, asBinary ? person.getName() : null);
        result.put(Person.PERSON_VAL, asBinary ? null : person);
        return result;
    }

    public Map.Entry<Integer, Person> entry(int key, Person person) {
        return new AbstractMap.SimpleImmutableEntry<>(key, person);
    }
}
