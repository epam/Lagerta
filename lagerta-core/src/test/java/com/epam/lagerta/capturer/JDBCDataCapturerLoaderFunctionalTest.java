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

package com.epam.lagerta.capturer;

import com.epam.lagerta.base.EntityDescriptor;
import com.epam.lagerta.base.jdbc.JDBCUtil;
import com.epam.lagerta.base.jdbc.committer.JDBCBaseFunctionalTest;
import com.epam.lagerta.base.jdbc.common.Person;
import com.epam.lagerta.base.jdbc.common.PersonEntries;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class JDBCDataCapturerLoaderFunctionalTest extends JDBCBaseFunctionalTest {

    private static final String DATA_PROVIDER_NOT_PERSON = "notPersonData";
    private static final String DATA_PROVIDER_LIST_VAL_NAME = "listValues";

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

    @Test
    public void notFoundDataForKey() throws Exception {
        JDBCDataCapturerLoader jdbcDataCapturerLoader = PersonEntries
                .getPersonOnlyJDBCDataCapturerLoader(dataSource);
        Object load = jdbcDataCapturerLoader.load(Person.PERSON_CACHE, 1);
        assertNull(load);
    }

    @Test
    public void loadNotBinaryDataForKey() throws Exception {
        JDBCDataCapturerLoader jdbcDataCapturerLoader = PersonEntries
                .getPersonOnlyJDBCDataCapturerLoader(dataSource);
        int key = 1;
        Person expectedPerson = new Person(1, "Name");
        JDBCUtil.insertIntoPersonTable(dataSource, key, expectedPerson, null, null);
        Object actual = jdbcDataCapturerLoader.load(Person.PERSON_CACHE, key);
        assertEquals(actual, expectedPerson);
    }

    @Test(dataProvider = DATA_PROVIDER_NOT_PERSON, expectedExceptions = RuntimeException.class)
    public void notLoadIncorrectTypeDataForKey(Integer key, Object notPersonVal) throws Exception {
        JDBCDataCapturerLoader jdbcDataCapturerLoader = PersonEntries
                .getPersonOnlyJDBCDataCapturerLoader(dataSource);
        JDBCUtil.insertIntoPersonTable(dataSource, key, notPersonVal, null, null);
        jdbcDataCapturerLoader.load(Person.PERSON_CACHE, key);
    }

    @Test(dataProvider = DATA_PROVIDER_VAL_NAME)
    public void loadValForKey(Integer key, Object val, String personName, Integer personId) throws Exception {
        EntityDescriptor<Object> entityDescriptor = new EntityDescriptor<>(Object.class,
                PersonEntries.getPersonFieldDescriptor(), Person.PERSON_TABLE, Person.PERSON_KEY);
        Map<String, EntityDescriptor> personEntityDescriptor =
                Collections.singletonMap(Person.PERSON_CACHE, entityDescriptor);

        JDBCDataCapturerLoader jdbcDataCapturerLoader = new JDBCDataCapturerLoader(dataSource, personEntityDescriptor);
        JDBCUtil.insertIntoPersonTable(dataSource, key, val, personName, personId);

        Object actual = jdbcDataCapturerLoader.load(Person.PERSON_CACHE, key);
        assertEquals(actual, val);
    }

    @Test
    public void loadBinaryObject() throws Exception {
        JDBCDataCapturerLoader jdbcDataCapturerLoader = PersonEntries
                .getPersonOnlyJDBCDataCapturerLoader(dataSource);
        int key = 22;
        Person expectedPerson = new Person(2, "Name2");
        JDBCUtil.insertIntoPersonTable(dataSource, key, null, expectedPerson.getName(), expectedPerson.getId());
        Object actual = jdbcDataCapturerLoader.load(Person.PERSON_CACHE, key);
        assertEquals(actual, expectedPerson);
    }

    @Test(dataProvider = DATA_PROVIDER_LIST_VAL_NAME)
    public void loadAllVal(Iterable<Integer> keys, Iterable<Object> values, Class<?> clazz) throws Exception {
        EntityDescriptor<?> entityDescriptor = new EntityDescriptor<>(clazz,
                PersonEntries.getPersonFieldDescriptor(), Person.PERSON_TABLE, Person.PERSON_KEY);
        Map<String, EntityDescriptor> personEntityDescriptor =
                Collections.singletonMap(Person.PERSON_CACHE, entityDescriptor);

        JDBCDataCapturerLoader jdbcDataCapturerLoader = new JDBCDataCapturerLoader(dataSource, personEntityDescriptor);
        Iterator<Integer> keysIterator = keys.iterator();
        Iterator<Object> valuesIterator = values.iterator();
        Map<Integer, Object> expectedResult = new HashMap<>();
        while (keysIterator.hasNext() && valuesIterator.hasNext()) {
            Integer nextKey = keysIterator.next();
            Object nextVal = valuesIterator.next();
            JDBCUtil.insertIntoPersonTable(dataSource, nextKey, nextVal, null, null);
            expectedResult.put(nextKey, nextVal);
        }

        Map<Integer, Object> actual = jdbcDataCapturerLoader.loadAll(Person.PERSON_CACHE, keys);
        assertEquals(actual, expectedResult);
    }

    @Test
    public void loadAllBinaryObject() throws Exception {
        JDBCDataCapturerLoader jdbcDataCapturerLoader = PersonEntries
                .getPersonOnlyJDBCDataCapturerLoader(dataSource);
        List<Integer> keys = Arrays.asList(1, 2, 3);
        List<Person> values = Arrays.asList(new Person(1, "Name1"), new Person(2, "Name2"), new Person(3, "Name3"));
        Iterator<Integer> keysIterator = keys.iterator();
        Iterator<Person> valuesIterator = values.iterator();
        Map<Integer, Person> expectedResult = new HashMap<>();
        while (keysIterator.hasNext() && valuesIterator.hasNext()) {
            Integer nextKey = keysIterator.next();
            Person nextVal = valuesIterator.next();
            JDBCUtil.insertIntoPersonTable(dataSource, nextKey, null, nextVal.getName(), nextVal.getId());
            expectedResult.put(nextKey, nextVal);
        }
        Map<Integer, Object> actual = jdbcDataCapturerLoader.loadAll(Person.PERSON_CACHE, keys);
        assertEquals(actual, expectedResult);
    }
}
