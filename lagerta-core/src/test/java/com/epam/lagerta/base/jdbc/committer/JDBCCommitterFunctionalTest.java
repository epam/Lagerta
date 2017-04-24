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
package com.epam.lagerta.base.jdbc.committer;

import com.epam.lagerta.base.jdbc.DataProviders;
import com.epam.lagerta.base.jdbc.JDBCUtil;
import com.epam.lagerta.base.jdbc.common.KeyValueAndMetadata;
import com.epam.lagerta.util.JDBCKeyValueMapper;
import com.google.common.collect.Iterators;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JDBCCommitterFunctionalTest extends JDBCBaseFunctionalTest {
    private JDBCCommitter jdbcCommitter;

    @BeforeMethod
    public void setUpCommitter() {
        jdbcCommitter = JDBCUtil.getJDBCCommitter(dataSource);
    }

    @Test(dataProvider = DataProviders.KV_META_LIST_PROVIDER)
    public void commit(List<KeyValueAndMetadata> data) throws SQLException {
        Map<Integer, Map<String, Object>> expectedResults = data
                .stream()
                .collect(Collectors.toMap(KeyValueAndMetadata::getKey, KeyValueAndMetadata::getKeyValueMap));
        String cache = data.get(0).getCache();
        String table = data.get(0).getTable();
        KeyValueAndMetadata.ResultMapGetter resultMapGetter = data.get(0).getResultMapGetter();
        Iterator<String> caches = Iterators.singletonIterator(cache);
        Iterator<List> keys = Iterators.singletonIterator(data
                .stream()
                .map(KeyValueAndMetadata::getKey)
                .collect(Collectors.toList()));
        Iterator<List<?>> values = Iterators.singletonIterator(data
                .stream()
                .map(KeyValueAndMetadata::getValue)
                .collect(Collectors.toList()));

        jdbcCommitter.commit(caches, keys, values);

        String selectQuery = String.format(JDBCUtil.SELECT_FROM_TEMPLATE, table);
        JDBCUtil.applyInConnection(dataSource, connection -> {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(selectQuery)) {
                while (resultSet.next()) {
                    Map<String, Object> map = resultMapGetter.getResultMap(resultSet);
                    int key = (int) map.get(JDBCKeyValueMapper.KEY_FIELD_NAME);
                    Map<String, Object> expectedMap = expectedResults.get(key);

                    Assert.assertEquals(map.size(), expectedMap.size());
                    map.forEach((k, v) -> Assert.assertEquals(v, expectedMap.get(k)));
                }
            }
        });
    }
}
