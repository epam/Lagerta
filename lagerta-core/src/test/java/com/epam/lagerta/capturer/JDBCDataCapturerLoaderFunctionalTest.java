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

package com.epam.lagerta.capturer;

import com.epam.lagerta.base.EntityDescriptor;
import com.epam.lagerta.base.jdbc.DataProviders;
import com.epam.lagerta.base.jdbc.JDBCUtil;
import com.epam.lagerta.base.jdbc.committer.JDBCBaseFunctionalTest;
import com.epam.lagerta.base.jdbc.common.KeyValueAndMetadata;
import com.epam.lagerta.base.jdbc.common.PrimitivesHolder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class JDBCDataCapturerLoaderFunctionalTest extends JDBCBaseFunctionalTest {

    @DataProvider(name = DataProviders.KV_META_PROVIDER)
    public static Object[][] provideKVMeta() {
        return DataProviders.provideKVMeta(ignite);
    }

    private JDBCDataCapturerLoader jdbcDataCapturerLoader;

    @BeforeMethod
    public void setUpDataCapturerLoader() {
        jdbcDataCapturerLoader = JDBCUtil.getJDBCDataCapturerLoader(dataSource);
    }

    @Test
    public void notFoundDataForKey() throws SQLException {
        Object load = jdbcDataCapturerLoader.load(PrimitivesHolder.CACHE, 1);
        assertNull(load);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void notLoadIncorrectValTypeForKey() throws SQLException {
        KeyValueAndMetadata incorrect = PrimitivesHolder.withMetaData(1, DataProviders.PH_1);

        incorrect.getKeyValueMap().put(EntityDescriptor.VAL_FIELD_NAME, new Object());
        JDBCUtil.insertIntoDB(dataSource, incorrect);
        jdbcDataCapturerLoader.load(incorrect.getCacheName(), incorrect.getKey());
    }

    @Test(dataProvider = DataProviders.KV_META_PROVIDER)
    public void load(KeyValueAndMetadata kvMeta) throws SQLException {
        JDBCUtil.insertIntoDB(dataSource, kvMeta);
        Object actual = jdbcDataCapturerLoader.load(kvMeta.getCacheName(), kvMeta.getKey());
        assertEquals(actual, kvMeta.getUnwrappedValue());
    }

    @Test(dataProvider = DataProviders.KV_META_PROVIDER)
    public void loadWithDefaultEntityDescriptor(KeyValueAndMetadata kvMeta) {
        JDBCUtil.insertIntoDB(dataSource, kvMeta);
        String cacheName = kvMeta.getCacheName();
        EntityDescriptor<?> descriptor = new EntityDescriptor<>(Object.class, kvMeta.getTableName());
        jdbcDataCapturerLoader = new JDBCDataCapturerLoader(
                dataSource, Collections.singletonMap(cacheName, descriptor));
        Object actual = jdbcDataCapturerLoader.load(cacheName, kvMeta.getKey());
        assertEquals(actual, kvMeta.getUnwrappedValue());
    }

    @Test(dataProvider = DataProviders.KV_META_PROVIDER)
    public void loadWithParsedEntityDescriptor(KeyValueAndMetadata kvMeta) {
        JDBCUtil.insertIntoDB(dataSource, kvMeta);
        String cacheName = kvMeta.getCacheName();
        Class<?> clazz = kvMeta.getUnwrappedValue().getClass();
        EntityDescriptor<?> descriptor = new EntityDescriptor<>(clazz, kvMeta.getTableName());
        jdbcDataCapturerLoader = new JDBCDataCapturerLoader(
                dataSource, Collections.singletonMap(cacheName, descriptor));
        Object actual = jdbcDataCapturerLoader.load(cacheName, kvMeta.getKey());
        assertEquals(actual, kvMeta.getUnwrappedValue());
    }

    @Test(dataProvider = DataProviders.KV_META_LIST_PROVIDER)
    public void loadAll(List<KeyValueAndMetadata> kvMetas) {
        Map<Integer, Object> expectedResult = kvMetas
                .stream()
                .collect(Collectors.toMap(KeyValueAndMetadata::getKey,
                        KeyValueAndMetadata::getUnwrappedValue));
        JDBCUtil.applyInConnection(dataSource, connection -> {
            for (KeyValueAndMetadata kvMeta : kvMetas) {
                JDBCUtil.insertIntoDB(connection, kvMeta);
            }
        });
        String cache = kvMetas.get(0).getCacheName();
        Map<Integer, Object> actual = jdbcDataCapturerLoader.loadAll(cache, expectedResult.keySet());
        assertEquals(actual, expectedResult);
    }
}
