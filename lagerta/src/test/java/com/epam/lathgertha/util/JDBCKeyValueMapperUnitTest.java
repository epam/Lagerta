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
package com.epam.lathgertha.util;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JDBCKeyValueMapperUnitTest {
    private static final String SHOULD_BE_SERIALIZED_PROVIDER = "shouldBeSerializedProvider";
    private static final String SHOULD_NOT_BE_SERIALIZED_PROVIDER = "shouldNotBeSerializedProvider";
    private static final String CORRECT_PARAMS_PROVIDER = "correctParamsMap";
    private static final String INCORRECT_PARAMS_PROVIDER = "incorrectParamsMap";

    private static class TestEntry {
        private int intField;
        private String stringField;
        private Object objectField;
        private Collection collectionField;

        public TestEntry() {
        }

        public TestEntry(int intField, String stringField, Object objectField, Collection collectionField) {
            this.intField = intField;
            this.stringField = stringField;
            this.objectField = objectField;
            this.collectionField = collectionField;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestEntry testEntry = (TestEntry) o;
            if (intField != testEntry.intField) return false;
            if (stringField != null ? !stringField.equals(testEntry.stringField) : testEntry.stringField != null)
                return false;
            if (objectField != null ? !objectField.equals(testEntry.objectField) : testEntry.objectField != null)
                return false;
            return collectionField != null ? collectionField.equals(testEntry.collectionField) : testEntry.collectionField == null;
        }
    }

    @DataProvider(name = SHOULD_NOT_BE_SERIALIZED_PROVIDER)
    public Object[][] provideForShouldNotBeSerialized() {
        Date time = Calendar.getInstance().getTime();
        return new Object[][]{
            {TimeUnit.DAYS},
            {0},
            {(short) 0},
            {(byte) 0},
            {0L},
            {0.0D},
            {0.0F},
            {'c'},
            {"string"},
            {time},
            {null}
        };
    }

    @Test(dataProvider = SHOULD_NOT_BE_SERIALIZED_PROVIDER)
    public void shouldBeSerializedForDBFalseExpected(Object object) throws Exception {
        assertFalse(JDBCKeyValueMapper.shouldBeSerializedForDB(object));
    }

    @DataProvider(name = SHOULD_BE_SERIALIZED_PROVIDER)
    public Object[][] provideForShouldBeSerialized() {
        return new Object[][]{
            {new int[3]},
            {new float[]{0.0F}},
            {new Object()},
            {new TestEntry(1, "", new Object(), Collections.emptyList())},
            {Collections.emptyList()},
            {Collections.emptyMap()},
            {Collections.emptySet()}
        };
    }

    @Test(dataProvider = SHOULD_BE_SERIALIZED_PROVIDER)
    public void shouldBeSerializedForDBTrueExpected(Object object) throws Exception {
        assertTrue(JDBCKeyValueMapper.shouldBeSerializedForDB(object));
    }

    @Test
    public void valEntryWroteToMap() throws Exception {
        TestEntry entryValue = new TestEntry(1, "name", new Object(), Collections.emptyList());
        int key = 10;
        Map<String, Object> actualResult = JDBCKeyValueMapper.keyValueMap(key, entryValue);
        Map<String, Object> expectedResult = new HashMap<String, Object>() {{
            put(JDBCKeyValueMapper.KEY_FIELD_NAME, key);
            put(JDBCKeyValueMapper.VAL_FIELD_NAME, entryValue);
        }};
        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void binaryObjectFieldsWroteToMap() throws Exception {
        BinaryObject binaryValue = mock(BinaryObject.class);
        BinaryType mockBinaryType = mock(BinaryType.class);
        Collection<String> fieldNames = Stream.of("v1", "v2").collect(Collectors.toList());
        doReturn(mockBinaryType).when(binaryValue).type();
        doReturn(fieldNames).when(mockBinaryType).fieldNames();

        int key = 10;
        Map<String, Object> expectedValues = new HashMap<>(3);
        expectedValues.put("v1", "v1");
        expectedValues.put("v2", 2);
        expectedValues.put(JDBCKeyValueMapper.KEY_FIELD_NAME, key);

        when(binaryValue.field(anyString())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            return expectedValues.get(args[0]);
        });
        Map<String, Object> actualResult = JDBCKeyValueMapper.keyValueMap(key, binaryValue);

        assertEquals(actualResult, expectedValues);
    }

    @DataProvider(name = CORRECT_PARAMS_PROVIDER)
    public Object[][] provideCorrectParamsMap() {
        Object expectedEntry = new TestEntry(1, "1", "Object", Collections.singletonList(1));
        Map<String, Object> testEntryParams = new HashMap<>();
        testEntryParams.put("intField", 1);
        testEntryParams.put("stringField", "1");
        testEntryParams.put("objectField", "Object");
        testEntryParams.put("collectionField", Collections.singletonList(1));
        Date expectedDate = new Date(System.currentTimeMillis());
        return new Object[][]{
                {testEntryParams, TestEntry.class, expectedEntry},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, expectedEntry), TestEntry.class, expectedEntry},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, 1), Integer.class, 1},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, 1), int.class, 1},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, (short) 1), Short.class, (short) 1},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, (short) 1), short.class, (short) 1},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, 1L), Long.class, 1L},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, 1L), long.class, 1L},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, 1F), Float.class, 1F},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, 1F), float.class, 1F},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, 1D), Double.class, 1D},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, 1D), double.class, 1D},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, "Hello"), String.class, "Hello"},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, expectedDate), Date.class, expectedDate},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, Collections.singletonList(1)),
                        List.class, Collections.singletonList(1)},

                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, new ArrayList<>()),
                        List.class, new ArrayList<>()},

                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, Collections.singletonMap("Key", 1)),
                        Map.class, Collections.singletonMap("Key", 1)},

                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, new HashMap<>()),
                        Map.class, new HashMap<>()}
        };
    }

    @Test(dataProvider = CORRECT_PARAMS_PROVIDER)
    public <T> void createFromMapWithCorrectParams(Map<String, Object> params, Class<T> clazz, T expectedObject) {
        T actualObject = JDBCKeyValueMapper.getObject(params, clazz);
        assertEquals(expectedObject, actualObject);
    }

    @DataProvider(name = INCORRECT_PARAMS_PROVIDER)
    public Object[][] provideIncorrectParamsMap() {
        Map<String, Object> incorrectTestEntryParams = new HashMap<>();
        incorrectTestEntryParams.put("notInt", 1);
        incorrectTestEntryParams.put("NotString", "1");
        incorrectTestEntryParams.put("buzz", "Object");
        incorrectTestEntryParams.put("collectionField", Collections.singletonList(1));
        return new Object[][]{
                {incorrectTestEntryParams, TestEntry.class},
                {Collections.<String, Object>singletonMap("f", 1), Integer.class},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, 1), TestEntry.class},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, new TestEntry()), String.class},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, new TestEntry()), Date.class},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, new Date()), TestEntry.class},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, "hello"), TestEntry.class},
                {Collections.<String, Object>singletonMap(JDBCKeyValueMapper.VAL_FIELD_NAME, "1"), Integer.class}
        };
    }

    @Test(dataProvider = INCORRECT_PARAMS_PROVIDER, expectedExceptions = RuntimeException.class)
    public <T> void createFromMapWithIncorrectParams(Map<String, Object> params, Class<T> clazz) throws RuntimeException {
        JDBCKeyValueMapper.getObject(params, clazz);
    }
}
