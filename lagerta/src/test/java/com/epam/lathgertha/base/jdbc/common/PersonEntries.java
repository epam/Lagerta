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

import com.epam.lathgertha.base.EntityDescriptor;
import com.epam.lathgertha.base.FieldDescriptor;
import com.epam.lathgertha.base.jdbc.committer.JDBCCommitter;
import com.epam.lathgertha.capturer.JDBCDataCapturerLoader;
import com.epam.lathgertha.util.Serializer;
import com.epam.lathgertha.util.SerializerImpl;

import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialBlob;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PersonEntries {
    private static final Serializer SERIALIZER = new SerializerImpl();
    private static final String SQL_BASE_PATH = "/com/epam/lathgertha/base/jdbc/committer/";

    public static final String CREATE_TABLE_SQL_RESOURCE = SQL_BASE_PATH + "create_tables.sql";
    public static final String DROP_TABLE_SQL_RESOUCE = SQL_BASE_PATH + "drop_tables.sql";

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

    public static JDBCCommitter getPersonOnlyJDBCCommitter(DataSource dataSource) {
        Map<String, EntityDescriptor> personEntityDescriptor = Collections.singletonMap(Person.PERSON_CACHE, getPersonEntityDescriptor());
        return new JDBCCommitter(dataSource, personEntityDescriptor);
    }

    public static JDBCDataCapturerLoader getPersonOnlyJDBCDataCapturerLoader(DataSource dataSource) {
        Map<String, EntityDescriptor> personEntityDescriptor =
                Collections.singletonMap(Person.PERSON_CACHE, getPersonEntityDescriptor());
        return new JDBCDataCapturerLoader(dataSource, personEntityDescriptor);
    }

    public static List<String> getPersonColumns() {
        return getPersonFieldDescriptor().keySet()
                .stream()
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

    public static EntityDescriptor getPersonEntityDescriptor() {
        return new EntityDescriptor<>(Person.class, getPersonFieldDescriptor(), Person.PERSON_TABLE, Person.PERSON_KEY);
    }

    private static FieldDescriptor PERSON_ID_DESCRIPTOR = new FieldDescriptor() {

        @Override
        public int getIndex() {
            return Person.PERSON_ID_INDEX;
        }

        @Override
        public void setValueInStatement(Object object, PreparedStatement preparedStatement) throws SQLException {
            if (object == null) {
                preparedStatement.setObject(getIndex(), null);
            } else {
                preparedStatement.setInt(getIndex(), (Integer) object);
            }
        }

        @Override
        public Object getFieldValue(ResultSet resultSet) {
            try {
                return resultSet.getInt(getIndex());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private static FieldDescriptor PERSON_NAME_DESCRIPTOR = new FieldDescriptor() {

        @Override
        public int getIndex() {
            return Person.PERSON_NAME_INDEX;
        }

        @Override
        public void setValueInStatement(Object object, PreparedStatement preparedStatement) throws SQLException {
            if (object == null) {
                preparedStatement.setObject(getIndex(), null);
            } else {
                preparedStatement.setString(getIndex(), (String) object);
            }
        }

        @Override
        public Object getFieldValue(ResultSet resultSet) {
            try {
                return resultSet.getString(getIndex());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private static FieldDescriptor PERSON_VAL_DESCRIPTOR = new FieldDescriptor() {

        @Override
        public int getIndex() {
            return Person.PERSON_VAL_INDEX;
        }

        @Override
        public void setValueInStatement(Object object, PreparedStatement preparedStatement) throws SQLException {
            if (object == null) {
                preparedStatement.setObject(getIndex(), null);
            } else {
                preparedStatement.setBlob(getIndex(), new SerialBlob(SERIALIZER.serialize(object).array()));
            }
        }

        @Override
        public Object getFieldValue(ResultSet resultSet) {
            try {
                Blob blob = resultSet.getBlob(getIndex());
                if (blob != null) {
                    ByteBuffer wrap = ByteBuffer.wrap(blob.getBytes(0, (int) blob.length()));
                    return SERIALIZER.deserialize(wrap);
                }
                return null;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private static FieldDescriptor PERSON_KEY_DESCRIPTOR = new FieldDescriptor() {

        @Override
        public int getIndex() {
            return Person.PERSON_KEY_INDEX;
        }

        @Override
        public void setValueInStatement(Object object, PreparedStatement preparedStatement) throws SQLException {
            if (object == null) {
                preparedStatement.setObject(getIndex(), null);
            } else {
                preparedStatement.setInt(getIndex(), (Integer) object);
            }
        }

        @Override
        public Object getFieldValue(ResultSet resultSet) {
            try {
                return resultSet.getInt(getIndex());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    };
}
