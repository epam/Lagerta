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

package com.epam.lagerta;

import com.epam.lagerta.base.EntityDescriptor;
import com.epam.lagerta.base.jdbc.JDBCUtil;
import com.epam.lagerta.base.jdbc.committer.JDBCCommitter;
import com.epam.lagerta.base.jdbc.common.Person;
import com.epam.lagerta.base.jdbc.common.PersonEntries;
import com.epam.lagerta.capturer.DataCapturerLoader;
import com.epam.lagerta.capturer.JDBCDataCapturerLoader;
import com.epam.lagerta.resources.DBResource;
import com.epam.lagerta.resources.FullClusterResource;
import com.epam.lagerta.subscriber.Committer;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    private static final String ADJUSTED_TOPIC_SUFFIX = "_adjusted_";

    private static final Map<String, EntityDescriptor> ENTITY_DESCRIPTOR_MAP = new HashMap<>();
    private static final DBResource DB_RESOURCE = new DBResource(DB_NAME);
    protected static final FullClusterResource ALL_RESOURCES = new FullClusterResource(DB_RESOURCE);

    static {
        ENTITY_DESCRIPTOR_MAP.put(BaseIntegrationTest.CACHE_NAME, PersonEntries.getPersonEntityDescriptor());
        ENTITY_DESCRIPTOR_MAP.put(BaseIntegrationTest.BINARY_KEEPING_CACHE_NAME, PersonEntries.getPersonEntityDescriptor());
    }

    private static int TEST_NUMBER = 0;

    protected DataSource dataSource;

    @DataProvider(name = CACHE_NAME_PROVIDER)
    public static Object[][] provideCacheName() {
        return new Object[][] {
                {CACHE_NAME},
                {BINARY_KEEPING_CACHE_NAME}
        };
    }

    private static Committer personJDBCCommitter() {
        return new JDBCCommitter(DB_RESOURCE.getDataSource(), ENTITY_DESCRIPTOR_MAP);
    }

    private static DataCapturerLoader personJDBCDataCapturerLoader() {
        return new JDBCDataCapturerLoader(DB_RESOURCE.getDataSource(), ENTITY_DESCRIPTOR_MAP);
    }

    public static String adjustTopicNameForTest(String topic) {
        if (topic.contains(ADJUSTED_TOPIC_SUFFIX)) {
            return topic;
        }
        return topic + ADJUSTED_TOPIC_SUFFIX + TEST_NUMBER;
    }


    @AfterSuite(alwaysRun = true)
    public void tearDown() {
        ALL_RESOURCES.tearDown();
    }

    @BeforeMethod
    public void initializeResources() throws SQLException {
        DB_RESOURCE.initState(PersonEntries.CREATE_TABLE_SQL_RESOURCE);
        dataSource = DB_RESOURCE.getDataSource();
    }

    @AfterMethod
    public void cleanupResources() throws SQLException {
        TEST_NUMBER++;
        ALL_RESOURCES.cleanUpClusters();
        DB_RESOURCE.clearState(PersonEntries.DROP_TABLE_SQL_RESOUCE);
    }

    public Ignite ignite() {
        return ALL_RESOURCES.igniteCluster().ignite();
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

    public void awaitTransactions() {
        Uninterruptibles.sleepUninterruptibly(TX_WAIT_TIME, TimeUnit.MILLISECONDS);
        LOGGER.debug("[T] SLEPT {}", TX_WAIT_TIME);
    }

    @SafeVarargs
    public final void assertObjectsInDB(boolean asBinary, Map.Entry<Integer, Person>... persons) throws SQLException {
        JDBCUtil.applyInConnection(dataSource, connection -> {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(PERSON_TABLE_SELECT)) {

                for (Map.Entry<Integer, Person> entry : persons) {
                    AssertJUnit.assertTrue("No data in person table", resultSet.next());

                    Map<String, Object> expectedMap = personEntryToMap(asBinary, entry);
                    Map<String, Object> actualMap = PersonEntries.getResultMapForPerson(resultSet);

                    AssertJUnit.assertEquals(expectedMap, actualMap);
                }
            }
        });
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
