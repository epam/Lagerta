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

package com.epam.lathgertha.subscriber.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.epam.lathgertha.subscriber.DataProviderUtil.list;
import static org.testng.Assert.assertEquals;

public class MergeUtilUnitTest {

    private static final String LIST_OF_LONGS = "listOfLongs";
    private static final String LIST_OF_LONG_LISTS = "listOfLongLists";
    private static final Comparator<Long> LONG_COMPARATOR = Long::compareTo;

    @DataProvider(name = LIST_OF_LONGS)
    private Object[][] provideListsOfLongs() {
        List<Long> expected = list(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L);
        return new Object[][]{
                {list(0L, 3L, 5L, 7L), list(1L, 2L, 4L, 6L), expected},
                {list(0L, 3L, 7L), list(1L, 2L, 4L, 5L, 6L), expected},
                {list(2L, 10L, 11L), list(0L, 5L), list(0L, 2L, 5L, 10L, 11L)},
                {list(), list(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L), expected},
                {list(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L), list(), expected},
                {list(), list(), list()}
        };
    }

    @Test(dataProvider = LIST_OF_LONGS)
    public void listsMerging(List<Long> a, List<Long> b, List<Long> expected) {
        MergeUtil.merge(a, b, LONG_COMPARATOR);
        assertEquals(a, expected);
    }

    @DataProvider(name = LIST_OF_LONG_LISTS)
    private Object[][] provideListsOfLongLists() {
        List<Long> expected = list(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L);
        return new Object[][]{
                {list(0L, 3L), list(list(1L, 2L), list(4L, 5L)), list(0L, 1L, 2L, 3L, 4L, 5L)},
                {list(0L, 3L, 7L), list(list(1L, 2L), list(4L, 5L, 6L)), expected},
                {list(2L, 10L, 11L), list(list(0L), list(5L)), list(0L, 2L, 5L, 10L, 11L)},
                {list(), list(list(0L), list(1L, 2L, 3L, 4L, 5L, 6L, 7L)), expected},
                {list(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L), list(), expected},
                {list(), list(), list()}
        };
    }

    @Test(dataProvider = LIST_OF_LONG_LISTS)
    public void multipleListsMerging(List<Long> a, List<List<Long>> b, List<Long> expected) {
        MergeUtil.mergeCollections(a, b, LONG_COMPARATOR);
        assertEquals(a, expected);
    }

    @Test
    public void mergeEvenCountOfListsInBuffer() {
        List<List<Long>> buffer = Collections.nCopies(258, list());
        MergeUtil.mergeCollections(list(), buffer, LONG_COMPARATOR);
        assertEquals(list(), list());
    }

    @Test
    public void mergeOddCountOfListsInBuffer() {
        List<List<Long>> buffer = Collections.nCopies(257, list());
        MergeUtil.mergeCollections(list(), buffer, LONG_COMPARATOR);
        assertEquals(list(), list());
    }
}