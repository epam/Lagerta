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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.apache.ignite.IgniteCache;
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

        assertObjectInDB(1, second, asBinary);
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

    private void assertObjectInDB(int key, PrimitivesHolder holder, boolean asBinary) throws SQLException {
        JDBCUtil.applyInConnection(dataSource, connection -> {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(PRIMITIVES_TABLE_SELECT)) {

                Assert.assertTrue(resultSet.next());

                Map<String, Object> expectedMap = PrimitivesHolder.toMap(key, holder, asBinary);
                Map<String, Object> actualMap = PrimitivesHolder.getResultMap(resultSet);

                Assert.assertEquals(expectedMap, actualMap);
            }
        });
    }
}
