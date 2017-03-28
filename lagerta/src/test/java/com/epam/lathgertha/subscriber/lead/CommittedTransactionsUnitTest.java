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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.epam.lathgertha.subscriber.DataProviderUtil.list;
import static org.testng.Assert.*;

public class CommittedTransactionsUnitTest {

    private static final String LIST_OF_TRANSACTIONS = "listOfTransactions";
    private static final long EXPECTED_LAST_DENSE_COMMITTED = 7L;

    private CommittedTransactions committed;

    @BeforeMethod
    public void setUp() {
        committed = new CommittedTransactions();
    }

    @DataProvider(name = LIST_OF_TRANSACTIONS)
    private Object[][] provideListsOfTransactions() {
        List<List<Long>> commonSizeLists = list(
                list(0L, 6L),
                list(5L, 7L),
                list(1L, 2L),
                list(100L, 101L),
                list(3L, 4L));
        List<List<Long>> diffSizeLists = list(
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

    @Test(dataProvider = LIST_OF_TRANSACTIONS)
    public void compressWorksWithMergedManyLists(List<List<Long>> transactions) {
        transactions.forEach(committed::addAll);
        committed.compress();
        long commitAfterCompress = committed.getLastDenseCommit();
        assertEquals(commitAfterCompress, EXPECTED_LAST_DENSE_COMMITTED);
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
