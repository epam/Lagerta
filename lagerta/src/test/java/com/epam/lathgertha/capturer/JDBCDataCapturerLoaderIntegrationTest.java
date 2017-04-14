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

package com.epam.lathgertha.capturer;

import com.epam.lathgertha.BaseIntegrationTest;
import com.epam.lathgertha.base.jdbc.JDBCUtil;
import com.epam.lathgertha.base.jdbc.common.Person;
import com.epam.lathgertha.subscriber.DataProviderUtil;
import org.apache.ignite.IgniteCache;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.epam.lathgertha.subscriber.DataProviderUtil.list;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class JDBCDataCapturerLoaderIntegrationTest extends BaseIntegrationTest {
    private static final String LOAD_PERSON_PROVIDER = "loadPersonProvider";
    private static final String LOAD_ALL_PERSONS_PROVIDER = "loadAllPersonsProvider";
    private static final int FIRST_KEY = 1;
    private static final int SECOND_KEY = 2;
    private static final int THIRD_KEY = 3;
    private static final Person FIRST_PERSON = new Person(1, "firstName");
    private static final Person SECOND_PERSON = new Person(2, "secondName");
    private static final Person THIRD_PERSON = new Person(3, "thirdName");
    private static final Set<Integer> ALL_KEYS = new HashSet<>(list(FIRST_KEY, SECOND_KEY, THIRD_KEY));

    @DataProvider(name = LOAD_PERSON_PROVIDER)
    public Object[][] provideForLoadPerson() {
        Object[][] data = new Object[][] {
                {descriptor(FIRST_KEY, FIRST_PERSON, false)},
                {descriptor(FIRST_KEY, FIRST_PERSON, true)}
        };
        return DataProviderUtil.concat(provideCacheName(), data);
    }

    @DataProvider(name = LOAD_ALL_PERSONS_PROVIDER)
    public Object[][] provideForLoadAllPersons() {
        Object[][] data = new Object[][] {
                {list(
                        descriptor(FIRST_KEY, FIRST_PERSON, false),
                        descriptor(SECOND_KEY, SECOND_PERSON, false),
                        descriptor(THIRD_KEY, THIRD_PERSON, false)
                )},
                {list(
                        descriptor(FIRST_KEY, FIRST_PERSON, true),
                        descriptor(SECOND_KEY, SECOND_PERSON, true),
                        descriptor(THIRD_KEY, THIRD_PERSON, true)
                )}
        };
        return DataProviderUtil.concat(provideCacheName(), data);
    }

    @Test(dataProvider = CACHE_NAME_PROVIDER)
    public void notFoundLoad(String cacheName) {
        IgniteCache<Integer, Person> cache = ignite().cache(cacheName);

        assertNull(cache.get(FIRST_KEY));
    }

    @Test(dataProvider = CACHE_NAME_PROVIDER)
    public void notFoundLoadAll(String cacheName) {
        IgniteCache<Integer, Person> cache = ignite().cache(cacheName);

        assertTrue(cache.getAll(ALL_KEYS).isEmpty());
    }

    @Test(dataProvider = LOAD_PERSON_PROVIDER)
    public void partiallyFoundLoadAll(String cacheName, PersonDescriptor descriptor) throws SQLException {
        JDBCUtil.insertIntoPersonTable(
                dataSource,
                descriptor.key,
                descriptor.value,
                descriptor.name,
                descriptor.id
        );

        IgniteCache<Integer, Person> cache = ignite().cache(cacheName);

        Map<Integer, Person> results = cache.getAll(ALL_KEYS);
        Map<Integer, Person> expected = Collections.singletonMap(descriptor.key, descriptor.person);

        assertEquals(results, expected);
    }

    @Test(dataProvider = LOAD_PERSON_PROVIDER)
    public void loadPerson(String cacheName, PersonDescriptor descriptor) throws SQLException {
        JDBCUtil.insertIntoPersonTable(dataSource, descriptor.key, descriptor.value, descriptor.name, descriptor.id);

        IgniteCache<Integer, Person> cache = ignite().cache(cacheName);

        assertEquals(descriptor.person, cache.get(FIRST_KEY));
    }

    @Test(dataProvider = LOAD_ALL_PERSONS_PROVIDER)
    public void loadAllPersons(String cacheName, List<PersonDescriptor> descriptors) throws SQLException {
        JDBCUtil.applyInConnection(dataSource, connection -> {
            for (PersonDescriptor descriptor : descriptors) {
                JDBCUtil.insertIntoPersonTable(
                        connection,
                        descriptor.key,
                        descriptor.value,
                        descriptor.name,
                        descriptor.id
                );
            }
        });

        Map<Integer, Person> expectedResults = descriptors
                .stream()
                .collect(Collectors.toMap(
                        descriptor -> descriptor.key,
                        descriptor -> descriptor.person)
                );
        IgniteCache<Integer, Person> cache = ignite().cache(cacheName);

        assertEquals(expectedResults, cache.getAll(ALL_KEYS));
    }

    private static PersonDescriptor descriptor(int key, Person person, boolean asBinary) {
        return new PersonDescriptor(
                key,
                person,
                asBinary ? null : person,
                asBinary ? person.getName() : null,
                asBinary ? person.getId() : person.getId()
        );
    }

    private static class PersonDescriptor {
        public final int key;
        public final Person person;
        public final Object value;
        public final String name;
        public final Integer id;

        PersonDescriptor(int key, Person person, Object value, String name, Integer id) {
            this.key = key;
            this.person = person;
            this.value = value;
            this.name = name;
            this.id = id;
        }
    }
}
