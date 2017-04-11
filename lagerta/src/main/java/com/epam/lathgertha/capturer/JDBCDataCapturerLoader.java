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

package com.epam.lathgertha.capturer;

import com.epam.lathgertha.base.EntityDescriptor;

import javax.cache.integration.CacheLoaderException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JDBCDataCapturerLoader implements DataCapturerLoader {
    private final Map<String, EntityDescriptor> entityDescriptors;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    public JDBCDataCapturerLoader(Map<String, EntityDescriptor> entityDescriptors,
                                  String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.entityDescriptors = entityDescriptors;
    }

    @Override
    public Object load(String cacheName, Object key) {
        EntityDescriptor entityDescriptor = getEntityDescriptor(cacheName);
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            PreparedStatement statement = conn.prepareStatement(entityDescriptor.getSelectQuery());
            return loadMapResult(Collections.singletonList(key), entityDescriptor, statement).get(key);
        } catch (Exception e) {
            throw new CacheLoaderException(e);
        }
    }

    @Override
    public <K, V> Map<K, V> loadAll(String cacheName, Iterable<? extends K> keys) throws CacheLoaderException {
        EntityDescriptor entityDescriptor = getEntityDescriptor(cacheName);
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            PreparedStatement statement = conn.prepareStatement(entityDescriptor.getSelectQuery());
            return loadMapResult(keys, entityDescriptor, statement);
        } catch (Exception e) {
            throw new CacheLoaderException(e);
        }
    }

    private <K, V> Map<K, V> loadMapResult(Iterable<? extends K> keys, EntityDescriptor entityDescriptor, PreparedStatement statement) throws Exception {
        List<K> keysInList = new ArrayList<>();
        keys.iterator().forEachRemaining(keysInList::add);
        statement.setObject(1, keysInList.toArray());
        ResultSet resultSet = statement.executeQuery();
        return entityDescriptor.transform(resultSet);
    }

    private EntityDescriptor getEntityDescriptor(String cacheName) {
        EntityDescriptor entityDescriptor = entityDescriptors.get(cacheName);
        if (entityDescriptor == null) {
            //todo issues #93
            throw new RuntimeException("Not found entityDescriptor for cache: " + cacheName);
        }
        return entityDescriptor;
    }
}
