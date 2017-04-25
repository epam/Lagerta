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

package com.epam.lagerta.subscriber;

import com.epam.lagerta.BaseSingleJVMIntegrationTest;
import com.epam.lagerta.base.jdbc.DataProviders;
import com.epam.lagerta.base.jdbc.JDBCUtil;
import com.epam.lagerta.base.jdbc.common.PrimitivesHolder;
import com.epam.lagerta.capturer.IdSequencer;
import com.epam.lagerta.mocks.ProxyReconciler;
import com.google.common.util.concurrent.Uninterruptibles;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.SpringResource;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SubscriberIntegrationTest extends BaseSingleJVMIntegrationTest {
    private static final String CACHE_INFO_PROVIDER = "cacheInfoProvider";
    private static final String PRIMITIVES_TABLE_SELECT = String.format(
        JDBCUtil.SELECT_FROM_TEMPLATE,
        PrimitivesHolder.TABLE
    );

    @DataProvider(name = CACHE_INFO_PROVIDER)
    public static Object[][] provideCacheInformation() {
        return DataProviderUtil.concat(providePrimitivesCacheName(), new Object[][]{{false}, {true}});
    }

    @Test(dataProvider = CACHE_INFO_PROVIDER)
    public void sequentialTransactions(String cacheName, boolean asBinary) throws Exception {
        IgniteCache<Integer, PrimitivesHolder> cache = ignite().cache(cacheName);
        PrimitivesHolder first = DataProviders.PH_1;
        PrimitivesHolder second = DataProviders.PH_2;

        cache.put(1, first);
        cache.put(1, second);
        awaitTransactions();

        assertObjectsInDB(Collections.singletonMap(1, second), asBinary);
    }

    @Test(dataProvider = PRIMITIVES_CACHE_NAMES_PROVIDER)
    public void testWriteThroughAndReadThrough(String cacheName) throws Exception {
        PrimitivesHolder expected = DataProviders.PH_1;
        IgniteCache<Integer, PrimitivesHolder> cache = ignite().cache(cacheName);

        cache.put(1, expected);
        awaitTransactions();

        cache.withSkipStore().clear(1);

        PrimitivesHolder actual = cache.get(1);
        Assert.assertEquals(actual, expected);
    }

    private void assertObjectsInDB(Map<Integer, PrimitivesHolder> holders, boolean asBinary) throws SQLException {
        JDBCUtil.applyInConnection(dataSource, connection -> {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(PRIMITIVES_TABLE_SELECT)) {
                for (Map.Entry<Integer, PrimitivesHolder> entry : holders.entrySet()) {
                    Assert.assertTrue(resultSet.next(), "Not sufficient entries in result set");

                    Map<String, Object> expectedMap = PrimitivesHolder.toMap(entry.getKey(), entry.getValue(), asBinary);
                    Map<String, Object> actualMap = PrimitivesHolder.getResultMap(resultSet);

                    Assert.assertEquals(actualMap, expectedMap);
                }
            }
        });
    }

    @Test
    public void gapDetectionProcessFillsGaps() throws Exception {
        int firstKey = 1;
        PrimitivesHolder firstValue = new PrimitivesHolder();
        IgniteCache<Integer, PrimitivesHolder> cache = ignite().cache(PrimitivesHolder.CACHE);
        cache.put(firstKey, firstValue);

        long missedTx = createGap();
        int lastKey = 2;
        PrimitivesHolder lastValue = new PrimitivesHolder(true, (byte) 1, (short) 1, 1, 1, 1, 1);
        cache.put(lastKey, lastValue);

        awaitReconciliationOnTransaction(missedTx);
        awaitTransactions();
        Map<Integer, PrimitivesHolder> expected = new LinkedHashMap<>();
        expected.put(firstKey, firstValue);
        expected.put(lastKey, lastValue);
        assertObjectsInDB(expected, false);
    }

    private void awaitReconciliationOnTransaction(long missedTx) {
        boolean wasReconciliationCalled;
        do {
            Collection<Boolean> gapDetectionResult = ignite().compute().broadcast(new ReconCheckerCallable(missedTx));
            wasReconciliationCalled = gapDetectionResult.stream().anyMatch(Boolean::booleanValue);
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } while (!wasReconciliationCalled);
    }

    private long createGap() {
        return getBean(IdSequencer.class).getNextId();
    }

    private static class ReconCheckerCallable implements IgniteCallable<Boolean> {
        @SpringResource(resourceClass = ProxyReconciler.class)
        private transient ProxyReconciler reconciler;

        private final long missedTx;

        public ReconCheckerCallable(long missedTx) {
            this.missedTx = missedTx;
        }

        @Override
        public Boolean call() throws Exception {
            return reconciler.wasReconciliationCalled(missedTx);
        }
    }
}
