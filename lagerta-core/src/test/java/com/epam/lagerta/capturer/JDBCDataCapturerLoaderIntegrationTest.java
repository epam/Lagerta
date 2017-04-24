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

package com.epam.lagerta.capturer;

import com.epam.lagerta.BaseSingleJVMIntegrationTest;
import com.epam.lagerta.base.jdbc.DataProviders;
import com.epam.lagerta.base.jdbc.JDBCUtil;
import com.epam.lagerta.base.jdbc.common.KeyValueAndMetadata;
import com.epam.lagerta.base.jdbc.common.PrimitivesHolder;
import org.apache.ignite.IgniteCache;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class JDBCDataCapturerLoaderIntegrationTest extends BaseSingleJVMIntegrationTest {
    @DataProvider(name = DataProviders.KV_META_PROVIDER)
    public static Object[][] provideKVMeta() {
        return DataProviders.provideKVMeta(ignite());
    }

    @DataProvider(name = DataProviders.KV_META_LIST_PROVIDER)
    public static Object[][] provideForJDBCCommit() {
        return DataProviders.provideKVMetaList(ignite());
    }

    @Test(dataProvider = PRIMITIVES_CACHE_NAMES_PROVIDER)
    public void notFoundLoad(String cacheName) {
        IgniteCache<Integer, PrimitivesHolder> cache = ignite().cache(cacheName);

        assertNull(cache.get(1));
    }

    @Test(dataProvider = PRIMITIVES_CACHE_NAMES_PROVIDER)
    public void notFoundLoadAll(String cacheName) {
        IgniteCache<Integer, PrimitivesHolder> cache = ignite().cache(cacheName);

        assertTrue(cache.getAll(DataProviders.KEYS).isEmpty());
    }

    @Test(dataProvider = DataProviders.KV_META_PROVIDER)
    public void partiallyFoundLoadAll(KeyValueAndMetadata kvMeta) throws SQLException {
        JDBCUtil.insertIntoDB(dataSource, kvMeta);

        IgniteCache<Integer, Object> cache = ignite().cache(kvMeta.getCache());

        Map<Integer, Object> results = cache.getAll(DataProviders.KEYS);
        Map<Integer, Object> expected = Collections.singletonMap(kvMeta.getKey(), kvMeta.getUnwrappedValue());

        assertEquals(results, expected);
    }

    @Test(dataProvider = DataProviders.KV_META_PROVIDER)
    public void loadPerson(KeyValueAndMetadata kvMeta) throws SQLException {
        JDBCUtil.insertIntoDB(dataSource, kvMeta);

        IgniteCache<Integer, Object> cache = ignite().cache(kvMeta.getCache());

        Object result = cache.get(kvMeta.getKey());
        Object expected = kvMeta.getUnwrappedValue();

        assertEquals(result, expected);
    }

    @Test(dataProvider = DataProviders.KV_META_LIST_PROVIDER)
    public void loadAllPersons(List<KeyValueAndMetadata> kvMetas) throws SQLException {
        JDBCUtil.applyInConnection(dataSource, connection -> {
            for (KeyValueAndMetadata kvMeta : kvMetas) {
                JDBCUtil.insertIntoDB(connection, kvMeta);
            }
        });
        Map<Integer, Object> expectedResults = kvMetas
                .stream()
                .collect(Collectors.toMap(KeyValueAndMetadata::getKey, KeyValueAndMetadata::getUnwrappedValue));
        String cacheName = kvMetas.get(0).getCache();
        IgniteCache<Integer, Object> cache = ignite().cache(cacheName);
        Map<Integer, Object> results = cache.getAll(expectedResults.keySet());

        assertEquals(results, expectedResults);
    }
}
