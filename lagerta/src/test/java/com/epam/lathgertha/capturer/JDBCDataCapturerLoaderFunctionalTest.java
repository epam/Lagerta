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

package com.epam.lathgertha.capturer;

import com.epam.lathgertha.base.EntityDescriptor;
import com.epam.lathgertha.base.jdbc.committer.JDBCBaseFunctionalTest;
import com.epam.lathgertha.base.jdbc.common.Person;
import com.epam.lathgertha.base.jdbc.common.PersonEntries;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertEquals;

public class JDBCDataCapturerLoaderFunctionalTest extends JDBCBaseFunctionalTest {

    @Test
    public void notFoundDataForKey() throws Exception {
        jdbcDataCapturerLoader = PersonEntries.getPersonOnlyJDBCDataCapturerLoader(dbUrl);
        Object load = jdbcDataCapturerLoader.load(Person.PERSON_CACHE, 1);
        assertNull(load);
    }

    @Test
    public void loadNotBinaryDataForKey() throws Exception {
        jdbcDataCapturerLoader = PersonEntries.getPersonOnlyJDBCDataCapturerLoader(dbUrl);
        int key = 1;
        Person expectedPerson = new Person(1, "Name");
        insertIntoToPersonTable(key, expectedPerson, null, null);
        Object actual = jdbcDataCapturerLoader.load(Person.PERSON_CACHE, key);
        assertEquals(actual, expectedPerson);
    }

    @Test(dataProvider = DATA_PROVIDER_NOT_PERSON, expectedExceptions = RuntimeException.class)
    public void notLoadIncorrectTypeDataForKey(Integer key, Object notPersonVal) throws Exception {
        jdbcDataCapturerLoader = PersonEntries.getPersonOnlyJDBCDataCapturerLoader(dbUrl);
        insertIntoToPersonTable(key, notPersonVal, null, null);
        jdbcDataCapturerLoader.load(Person.PERSON_CACHE, key);
    }

    @Test(dataProvider = DATA_PROVIDER_VAL_NAME)
    public void loadValForKey(Integer key, Object val, String personName, Integer personId) throws Exception {
        EntityDescriptor<Object> entityDescriptor = new EntityDescriptor<>(Object.class,
                PersonEntries.getPersonFieldDescriptor(), Person.PERSON_TABLE, Person.PERSON_KEY);
        Map<String, EntityDescriptor> personEntityDescriptor =
                Collections.singletonMap(Person.PERSON_CACHE, entityDescriptor);

        jdbcDataCapturerLoader = new JDBCDataCapturerLoader(personEntityDescriptor, dbUrl, "", "");
        insertIntoToPersonTable(key, val, personName, personId);

        Object actual = jdbcDataCapturerLoader.load(Person.PERSON_CACHE, key);
        assertEquals(actual, val);
    }

    @Test
    public void loadBinaryObject() throws Exception {
        jdbcDataCapturerLoader = PersonEntries.getPersonOnlyJDBCDataCapturerLoader(dbUrl);
        int key = 22;
        Person expectedPerson = new Person(2, "Name2");
        insertIntoToPersonTable(key, null, expectedPerson.getName(), expectedPerson.getId());
        Object actual = jdbcDataCapturerLoader.load(Person.PERSON_CACHE, key);
        assertEquals(actual, expectedPerson);
    }

    @Test(dataProvider = DATA_PROVIDER_LIST_VAL_NAME)
    public void loadAllVal(Iterable<Integer> keys, Iterable<Object> values, Class clazz) throws Exception {
        EntityDescriptor<?> entityDescriptor = new EntityDescriptor<>(clazz,
                PersonEntries.getPersonFieldDescriptor(), Person.PERSON_TABLE, Person.PERSON_KEY);
        Map<String, EntityDescriptor> personEntityDescriptor =
                Collections.singletonMap(Person.PERSON_CACHE, entityDescriptor);

        jdbcDataCapturerLoader = new JDBCDataCapturerLoader(personEntityDescriptor, dbUrl, "", "");
        Iterator<Integer> keysIterator = keys.iterator();
        Iterator<Object> valuesIterator = values.iterator();
        Map<Integer, Object> expectedResult = new HashMap<>();
        while (keysIterator.hasNext() && valuesIterator.hasNext()) {
            Integer nextKey = keysIterator.next();
            Object nextVal = valuesIterator.next();
            insertIntoToPersonTable(nextKey, nextVal, null, null);
            expectedResult.put(nextKey, nextVal);
        }

        Map<Integer, Object> actual = jdbcDataCapturerLoader.loadAll(Person.PERSON_CACHE, keys);
        assertEquals(actual, expectedResult);
    }

    @Test
    public void loadAllBinaryObject() throws Exception {
        jdbcDataCapturerLoader = PersonEntries.getPersonOnlyJDBCDataCapturerLoader(dbUrl);
        List<Integer> keys = Arrays.asList(1, 2, 3);
        List<Person> values = Arrays.asList(new Person(1, "Name1"), new Person(2, "Name2"), new Person(3, "Name3"));
        Iterator<Integer> keysIterator = keys.iterator();
        Iterator<Person> valuesIterator = values.iterator();
        Map<Integer, Person> expectedResult = new HashMap<>();
        while (keysIterator.hasNext() && valuesIterator.hasNext()) {
            Integer nextKey = keysIterator.next();
            Person nextVal = valuesIterator.next();
            insertIntoToPersonTable(nextKey, null, nextVal.getName(), nextVal.getId());
            expectedResult.put(nextKey, nextVal);
        }
        Map<Integer, Object> actual = jdbcDataCapturerLoader.loadAll(Person.PERSON_CACHE, keys);
        assertEquals(actual, expectedResult);
    }
}