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

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JDBCKeyValueMapperUnitTest {
    private static final String SHOULD_BE_SERIALIZED_PROVIDER = "shouldBeSerializedProvider";
    private static final String SHOULD_NOT_BE_SERIALIZED_PROVIDER = "shouldNotBeSerializedProvider";

    private static class TestEntry{
        final int intField;
        final String stringField;
        final Object objectField;
        final Collection collectionField;

        TestEntry(int intField, String stringField, Object objectField, Collection collectionField) {
            this.intField = intField;
            this.stringField = stringField;
            this.objectField = objectField;
            this.collectionField = collectionField;
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
}
