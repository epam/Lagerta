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
package com.epam.lathgertha.base.jdbc.common;

import com.epam.lathgertha.base.CacheInBaseDescriptor;
import com.epam.lathgertha.base.FieldDescriptor;
import com.epam.lathgertha.base.jdbc.committer.BaseMapper;
import com.epam.lathgertha.base.jdbc.committer.JDBCCommitter;
import com.epam.lathgertha.base.jdbc.committer.JDBCTransformer;
import com.epam.lathgertha.util.Serializer;
import com.epam.lathgertha.util.SerializerImpl;

import javax.sql.rowset.serial.SerialBlob;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PersonEntries {
    private static final Serializer SERIALIZER = new SerializerImpl();
    private static final String SQL_BASE_PATH = "/com/epam/lathgertha/base/jdbc/committer/";

    public static final String CREATE_TABLE_SQL_RESOURCE = SQL_BASE_PATH + "create_tables.sql";
    public static final String DROP_TABLE_SQL_RESOUCE = SQL_BASE_PATH + "clear_tables.sql";

    public static Map<String, Object> getResultMapForPerson(ResultSet resultSet) throws SQLException {
        Map<String, Object> actualResults = new HashMap<>(PersonEntries.getPersonColumns().size());
        actualResults.put(Person.PERSON_ID, resultSet.getInt(Person.PERSON_ID));
        actualResults.put(Person.PERSON_KEY, resultSet.getInt(Person.PERSON_KEY));
        Blob blob = resultSet.getBlob(Person.PERSON_VAL);
        Object deserializeVal = null;
        if (blob != null) {
            int length = (int) blob.length();
            deserializeVal = SERIALIZER.deserialize(ByteBuffer.wrap(blob.getBytes(1, length)));
        }
        actualResults.put(Person.PERSON_VAL, deserializeVal);
        actualResults.put(Person.PERSON_NAME, resultSet.getString(Person.PERSON_NAME));
        return actualResults;
    }

    public static JDBCCommitter getPersonOnlyJDBCCommitter(String dbUrl) {
        return new JDBCCommitter(Collections.singletonList(PersonEntries.getPersonCacheInBaseDescriptor()),
                Collections.singletonList(PersonEntries.getPersonMapper()), dbUrl, "", "");
    }

    public static List<String> getPersonColumns() {
        return getPersonFieldDescriptor().values()
                .stream()
                .map(FieldDescriptor::getName)
                .collect(Collectors.toList());
    }

    public static Map<String, FieldDescriptor> getPersonFieldDescriptor() {
        Map<String, FieldDescriptor> personFieldsDescriptor = new HashMap<>(4);
        personFieldsDescriptor.put(Person.PERSON_ID, PERSON_ID_DESCRIPTOR);
        personFieldsDescriptor.put(Person.PERSON_NAME, PERSON_NAME_DESCRIPTOR);
        personFieldsDescriptor.put(Person.PERSON_VAL, PERSON_VAL_DESCRIPTOR);
        personFieldsDescriptor.put(Person.PERSON_KEY, PERSON_KEY_DESCRIPTOR);
        return personFieldsDescriptor;
    }

    public static CacheInBaseDescriptor getPersonCacheInBaseDescriptor() {
        CacheInBaseDescriptor personMapper = new CacheInBaseDescriptor(Person.PERSON_CACHE, Person.PERSON_TABLE,
                new ArrayList<FieldDescriptor>(getPersonFieldDescriptor().values()));
        return personMapper;
    }

    public static BaseMapper getPersonMapper() {
        return new PersonMapper();
    }

    private static FieldDescriptor PERSON_ID_DESCRIPTOR = new FieldDescriptor() {
        @Override
        public String getName() {
            return Person.PERSON_ID;
        }

        @Override
        public JDBCTransformer transform(int index) {
            return (o, preparedStatement) -> preparedStatement.setObject(index, Integer.valueOf(o.toString()));
        }
    };

    private static FieldDescriptor PERSON_NAME_DESCRIPTOR = new FieldDescriptor() {
        @Override
        public String getName() {
            return Person.PERSON_NAME;
        }

        @Override
        public JDBCTransformer transform(int index) {
            return (o, preparedStatement) -> preparedStatement.setString(index, o.toString());
        }
    };

    private static FieldDescriptor PERSON_VAL_DESCRIPTOR = new FieldDescriptor() {
        private SerializerImpl serializer = new SerializerImpl();

        @Override
        public String getName() {
            return Person.PERSON_VAL;
        }

        @Override
        public JDBCTransformer transform(int index) {
            return (o, preparedStatement) -> {
                try {
                    preparedStatement.setBlob(index, new SerialBlob(serializer.serialize(o).array()));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            };
        }
    };

    private static FieldDescriptor PERSON_KEY_DESCRIPTOR = new FieldDescriptor() {
        @Override
        public String getName() {
            return Person.PERSON_KEY;
        }

        @Override
        public JDBCTransformer transform(int index) {
            return (o, preparedStatement) -> preparedStatement.setInt(index, Integer.valueOf(o.toString()));
        }
    };

    private static class PersonMapper implements BaseMapper {

        @Override
        public String getCacheName() {
            return Person.PERSON_CACHE;
        }

        private static String getParametersTemplate(int count) {
            List<String> buf = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                buf.add("?");
            }
            return String.join(",", buf);
        }

        @Override
        public PreparedStatement insertUpdateStatement(Connection connection, String tableName,
                                                       List<FieldDescriptor> fieldDescriptor) throws SQLException {
            String fieldsName = String.join(",", getPersonColumns());
            String parametersTemplate = getParametersTemplate(fieldDescriptor.size());
            String sql = "MERGE INTO " + tableName + " (" + fieldsName + ") KEY(" + Person.PERSON_KEY + ")" +
                    "VALUES (" + parametersTemplate + ")";
            return connection.prepareStatement(sql);
        }

        @Override
        public void addValuesToBatch(PreparedStatement statement,
                                     Map<String, Object> fieldValueMap) throws SQLException {
            for (String fieldName : getPersonColumns()) {
                int index = getPersonColumns().indexOf(fieldName) + 1;
                statement.setObject(index, null);
                Object value = fieldValueMap.get(fieldName);
                if (value != null) {
                    getPersonFieldDescriptor().get(fieldName).transform(index).accept(value, statement);
                }
            }
            statement.addBatch();
        }
    }
}
