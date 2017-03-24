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
package com.epam.lathgertha.base.jdbc.committer;

import com.epam.lathgertha.base.CacheInBaseDescriptor;
import com.epam.lathgertha.subscriber.Committer;
import com.epam.lathgertha.util.JDBCKeyValueMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JDBCCommitter implements Committer {

    private static final int BATCH_SIZE = 50_000;

    private final Map<String, CacheInBaseDescriptor> cacheDescriptorsMap;
    private final Map<String, BaseMapper> mappers;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    public JDBCCommitter(List<CacheInBaseDescriptor> cacheInBaseDescriptors, List<BaseMapper> mappers,
                         String dbUrl, String dbUser, String dbPassword) {
        this.cacheDescriptorsMap = new HashMap<>(cacheInBaseDescriptors.size());

        for (CacheInBaseDescriptor descriptor : cacheInBaseDescriptors) {
            this.cacheDescriptorsMap.put(descriptor.getCacheName(), descriptor);
        }

        this.mappers = new HashMap<>(cacheInBaseDescriptors.size());
        for (BaseMapper mapper : mappers) {
            this.mappers.put(mapper.getCacheName(), mapper);
        }
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    @Override
    public void commit(List<String> names, List<List<?>> keys, List<List<?>> values) {
        if (names.isEmpty()) {
            //no data for commit
            return;
        }
        Iterator<String> cachesIterator = names.iterator();
        Iterator<List<?>> keysIterator = keys.iterator();
        Iterator<List<?>> valuesIterator = values.iterator();

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            conn.setAutoCommit(false);
            executeBatches(cachesIterator, keysIterator, valuesIterator, conn);
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeBatches(Iterator<String> cachesIterator,
                                Iterator<List<?>> keysIterator,
                                Iterator<List<?>> valuesIterator,
                                Connection conn) throws SQLException {

        while (cachesIterator.hasNext() && keysIterator.hasNext() && valuesIterator.hasNext()) {
            String currentCache = cachesIterator.next();
            Iterator<?> currentKeys = keysIterator.next().iterator();
            Iterator<?> currentValues = valuesIterator.next().iterator();

            CacheInBaseDescriptor cacheInBaseDescriptor = cacheDescriptorsMap.get(currentCache);
            if (cacheInBaseDescriptor == null) {
                throw new RuntimeException("Not found cacheDescriptor for cache: " + currentCache);
            }
            BaseMapper mapper = mappers.get(currentCache);
            if (mapper == null) {
                throw new RuntimeException("Not set mapper for cache: " + currentCache);
            }

            try (PreparedStatement statement = mapper.insertUpdateStatement(conn,
                    cacheInBaseDescriptor.getTableName(),
                    cacheInBaseDescriptor.getFieldDescriptors())
            ) {
                int elementsInBatch = 0;
                while (currentKeys.hasNext() && currentValues.hasNext()) {
                    Object currentKey = currentKeys.next();
                    Object currentValue = currentValues.next();
                    Map<String, Object> fieldValueMap = JDBCKeyValueMapper.keyValueMap(currentKey, currentValue);
                    mapper.addValuesToBatch(statement, fieldValueMap);
                    if (++elementsInBatch >= BATCH_SIZE) {
                        statement.executeBatch();
                        elementsInBatch = 0;
                    }
                }
                if (elementsInBatch != 0) {
                    statement.executeBatch();
                }
            }

        }
    }
}
