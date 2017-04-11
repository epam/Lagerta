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

import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.subscriber.ConsumerTxScope;
import com.google.common.collect.Lists;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.epam.lathgertha.subscriber.DataProviderUtil.cacheScope;
import static com.epam.lathgertha.subscriber.DataProviderUtil.list;
import static com.epam.lathgertha.subscriber.DataProviderUtil.txScope;
import static org.testng.Assert.assertEquals;

public class ReadTransactionsUnitTest {

    private static final String LIST_OF_TRANSACTIONS = "listOfTransactions";
    private static final UUID NODE = java.util.UUID.randomUUID();
    private static final String CACHE = "cacheName";

    private static final CommittedTransactions COMMITTED = Mockito.mock(CommittedTransactions.class);
    private static final List<Map.Entry<String, List>> CACHE_SCOPE = Mockito.mock(List.class);

    private static final long LAST_DENSE_COMMITTED = 1L;
    private static final long EXPECTED_LAST_DENSE_READ = 4L;

    private ReadTransactions read;

    @BeforeClass
    public static void setUpClass() {
        Mockito.when(COMMITTED.getLastDenseCommit()).thenReturn(LAST_DENSE_COMMITTED);
    }

    @BeforeMethod
    public void setUp() {
        read = new ReadTransactions();
        read.setReadyAndPrune(COMMITTED);
    }

    @DataProvider(name = LIST_OF_TRANSACTIONS)
    private Object[][] provideListsOfTransactions() {
        List<ConsumerTxScope> denseRead = list(
                consumerTxScope(2),
                consumerTxScope(3),
                consumerTxScope(4));
        return new Object[][]{
                {commonSizeLists(), denseRead},
                {diffSizeLists(), denseRead}};
    }

    private List<List<TransactionScope>> commonSizeLists() {
        return list(
                list(txScope(0, cacheScope(CACHE, 0L, 6L)),
                        txScope(1, cacheScope(CACHE, 5L, 7L))),
                list(txScope(2, cacheScope(CACHE, 1L, 2L)),
                        txScope(3, cacheScope(CACHE, 100L, 101L))),
                list(txScope(4, cacheScope(CACHE, 3L, 4L))));
    }

    private List<List<TransactionScope>> diffSizeLists() {
        return list(
                list(txScope(0, cacheScope(CACHE, 0L, 15L, 100L, 101L, 102L))),
                list(txScope(1, cacheScope(CACHE))),
                list(txScope(2, cacheScope(CACHE, 1L, 2L, 10L, 103L))),
                list(txScope(3, cacheScope(CACHE, 3L, 5L))),
                list(txScope(4, cacheScope(CACHE, 4L))),
                list(txScope(6, cacheScope(CACHE, 90L, 98L, 99L))),
                list(txScope(9, cacheScope(CACHE, 50L, 77L, 78L, 104L))),
                list(txScope(11, cacheScope(CACHE, 51L, 79L, 80L, 97L, 105L, 106L))),
                list(txScope(12, cacheScope(CACHE, 6L, 7L, 42L, 49L))));
    }

    @Test(dataProvider = LIST_OF_TRANSACTIONS)
    public void pruningWorksWithAddManyLists(
            List<List<TransactionScope>> transactions,
            List<ConsumerTxScope> expectedDenseRead) {
        transactions.forEach(tx -> read.addAllOnNode(NODE, tx));
        read.pruneCommitted(COMMITTED);
        long commitAfterCompress = read.getLastDenseRead();
        assertEquals(commitAfterCompress, EXPECTED_LAST_DENSE_READ);
    }

    @Test(dataProvider = LIST_OF_TRANSACTIONS)
    public void iteratorReturnsReadDenseTransactions(
            List<List<TransactionScope>> transactions,
            List<ConsumerTxScope> expectedDenseRead) {
        transactions.forEach(tx -> read.addAllOnNode(NODE, tx));
        read.pruneCommitted(COMMITTED);
        List<Long> actual = Lists.newArrayList(read.iterator()).stream()
                .map(ConsumerTxScope::getTransactionId)
                .collect(Collectors.toList());
        List<Long> expected = expectedDenseRead.stream()
                .map(ConsumerTxScope::getTransactionId)
                .collect(Collectors.toList());
        assertEquals(actual, expected);
    }

    private static ConsumerTxScope consumerTxScope(long transactionId) {
        return new ConsumerTxScope(NODE, transactionId, CACHE_SCOPE);
    }
}