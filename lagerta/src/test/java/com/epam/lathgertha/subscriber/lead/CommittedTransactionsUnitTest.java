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
package com.epam.lathgertha.subscriber.lead;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CommittedTransactionsUnitTest {

    private static final String LIST_OF_TRANSACTIONS = "listOfTransactions";
    private static final String LIST_OF_LONGS = "listOfLongs";

    private CommittedTransactions committed;

    @BeforeMethod
    public void setUp() {
        committed = new CommittedTransactions();
    }

    @DataProvider(name = LIST_OF_TRANSACTIONS)
    private Object[][] provideListsOfTransactions() {
        List<List<Long>> commonSizeLists = Arrays.asList(
                list(0L, 6L),
                list(5L, 7L),
                list(1L, 2L),
                list(100L, 101L),
                list(3L, 4L));
        List<List<Long>> diffSizeLists = Arrays.asList(
                list(0L, 15L, 100L, 101L, 102L),
                list(),
                list(1L, 2L, 10L, 103L),
                list(3L, 5L),
                list(4L),
                list(90L, 98L, 99L),
                list(50L, 77L, 78L, 104L),
                list(51L, 79L, 80L, 97L, 105L, 106L),
                list(6L, 7L, 42L, 49L));

        return new Object[][]{{commonSizeLists}, {diffSizeLists}};
    }

    private List<Long> list(Long... longs) {
        return new ArrayList<>(Arrays.asList(longs));
    }

    @Test(dataProvider = LIST_OF_TRANSACTIONS)
    public void compressWorksWithMergedManyLists(List<List<Long>> transactions) {
        transactions.forEach(committed::addAll);
        committed.compress();
        long commitAfterCompress = committed.getLastDenseCommit();
        assertEquals(commitAfterCompress, 7L);
    }

    @DataProvider(name = LIST_OF_LONGS)
    private Object[][] provideListsOfLongs() {
        List<Long> expected = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L);
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
        CommittedTransactions.merge(a, b);
        assertEquals(a, expected);
    }

    @Test(dataProvider = LIST_OF_TRANSACTIONS)
    public void containsWorksWithCompressed(List<List<Long>> transactions) {
        transactions.forEach(committed::addAll);
        committed.compress();
        assertTrue(committed.contains(1L));
        assertFalse(committed.contains(8L));
    }

    @Test(dataProvider = LIST_OF_TRANSACTIONS)
    public void containsWorksWithOutOfCompressed(List<List<Long>> transactions) {
        transactions.forEach(committed::addAll);
        committed.compress();
        assertTrue(committed.contains(100L));
    }
}
