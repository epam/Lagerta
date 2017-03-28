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

import com.epam.lathgertha.base.jdbc.JDBCUtil;
import com.epam.lathgertha.base.jdbc.common.Person;
import com.epam.lathgertha.base.jdbc.common.PersonEntries;
import com.epam.lathgertha.resources.FullClusterResource;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseIntegrationTest.class);
    private static final String PERSON_TABLE_SELECT = String.format("SELECT * FROM %s ORDER BY ASC", Person.PERSON_TABLE);
    private static final long TX_WAIT_TIME = 1_000;

    private final FullClusterResource allResources = new FullClusterResource();

    @BeforeSuite
    public void setUp() throws Exception {
        allResources.setUp();
        try (Connection connection = allResources.getDBResource().getConnection()) {
            JDBCUtil.executeUpdateQueryFromResource(connection, PersonEntries.CREATE_TABLE_SQL_RESOURCE);
        }
    }

    @AfterSuite
    public void tearDown() {
        allResources.tearDown();
    }

    @AfterMethod
    public void cleanupResources() throws SQLException {
        allResources.cleanUpClusters();
    }

    public Ignite ignite() {
        return allResources.igniteCluster().ignite();
    }

    public <K, V> IgniteCache<K, V> getCache() {
        return ignite().cache(Person.PERSON_CACHE);
    }

    public void writePersonToCache(Ignite ignite, int key, Person person) {
        IgniteCache<Integer, Person> cache = ignite.cache(Person.PERSON_CACHE);

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(key, person);
            tx.commit();
        }
    }

    private void awaitTransaction() throws InterruptedException {
        Thread.sleep(TX_WAIT_TIME);
        LOGGER.debug("[T] SLEPT {}", TX_WAIT_TIME);
    }

    public void assertObjectsInDB(Map.Entry<Integer, Person>... persons) throws SQLException {
        try (Connection connection = allResources.getDBResource().getConnection()) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery(PERSON_TABLE_SELECT);

                for (Map.Entry<Integer, Person> entry : persons) {
                    AssertJUnit.assertTrue(resultSet.next());
                    AssertJUnit.assertEquals(personEntryToMap(entry), PersonEntries.getResultMapForPerson(resultSet));
                }
            }
        }
    }

    private static Map<String, Object> personEntryToMap(Map.Entry<Integer, Person> entry) {
        Map<String, Object> result = new HashMap<>(4);

        result.put(Person.PERSON_KEY, entry.getKey());
        result.put(Person.PERSON_ID, entry.getValue().getId());
        result.put(Person.PERSON_NAME, entry.getValue().getName());
        result.put(Person.PERSON_VAL, null);
        return result;
    }
}
