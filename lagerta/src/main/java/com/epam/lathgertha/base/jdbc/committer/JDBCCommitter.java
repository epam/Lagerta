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

import com.epam.lathgertha.base.EntityDescriptor;
import com.epam.lathgertha.subscriber.Committer;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.ConnectionPoolDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JDBCCommitter implements Committer {

    private static final int BATCH_SIZE = 50_000;

    private final BasicDataSource dataSource;
    private final Map<String, EntityDescriptor> entityDescriptors;

    public JDBCCommitter(BasicDataSource dataSource, Map<String, EntityDescriptor> entityDescriptors) {
        this.dataSource = dataSource;
        this.entityDescriptors = entityDescriptors;
    }

    @Override
    public void commit(Iterator<String> names, Iterator<List> keys, Iterator<List<?>> values) {
        if (!names.hasNext()) {
            //no data for commit
            return;
        }
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            executeBatches(names, keys, values, conn);
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeBatches(Iterator<String> cachesIterator,
                                Iterator<List> keysIterator,
                                Iterator<List<?>> valuesIterator,
                                Connection conn) throws SQLException {

        while (cachesIterator.hasNext() && keysIterator.hasNext() && valuesIterator.hasNext()) {
            String currentCache = cachesIterator.next();
            Iterator<?> currentKeys = keysIterator.next().iterator();
            Iterator<?> currentValues = valuesIterator.next().iterator();

            EntityDescriptor entityDescriptor = entityDescriptors.get(currentCache);
            if (entityDescriptor == null) {
                throw new RuntimeException("Not found cacheDescriptor for cache: " + currentCache);
            }
            try (PreparedStatement statement = conn.prepareStatement(entityDescriptor.getUpsertQuery())) {
                int elementsInBatch = 0;
                while (currentKeys.hasNext() && currentValues.hasNext()) {
                    Object currentKey = currentKeys.next();
                    Object currentValue = currentValues.next();
                    entityDescriptor.addValuesToBatch(currentKey, currentValue, statement);
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
