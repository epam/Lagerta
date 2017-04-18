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
package com.epam.lagerta.base.jdbc.common;

import com.epam.lagerta.base.BlobValueTransformer;
import com.epam.lagerta.base.EntityDescriptor;
import com.epam.lagerta.base.FieldDescriptor;
import com.epam.lagerta.base.jdbc.committer.JDBCCommitter;
import com.epam.lagerta.capturer.JDBCDataCapturerLoader;
import com.epam.lagerta.util.Serializer;
import com.epam.lagerta.util.SerializerImpl;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.epam.lagerta.base.SimpleValueTransformer.INTEGER;
import static com.epam.lagerta.base.SimpleValueTransformer.STRING;
import static com.epam.lagerta.base.jdbc.common.Person.PERSON_ID;
import static com.epam.lagerta.base.jdbc.common.Person.PERSON_ID_INDEX;
import static com.epam.lagerta.base.jdbc.common.Person.PERSON_KEY;
import static com.epam.lagerta.base.jdbc.common.Person.PERSON_KEY_INDEX;
import static com.epam.lagerta.base.jdbc.common.Person.PERSON_NAME;
import static com.epam.lagerta.base.jdbc.common.Person.PERSON_NAME_INDEX;
import static com.epam.lagerta.base.jdbc.common.Person.PERSON_VAL;
import static com.epam.lagerta.base.jdbc.common.Person.PERSON_VAL_INDEX;

public class PersonEntries {
    private static final Serializer SERIALIZER = new SerializerImpl();
    private static final String SQL_BASE_PATH = "/com/epam/lagerta/base/jdbc/committer/";

    public static final String CREATE_TABLE_SQL_RESOURCE = SQL_BASE_PATH + "create_tables.sql";
    public static final String DROP_TABLE_SQL_RESOUCE = SQL_BASE_PATH + "drop_tables.sql";

    public static Map<String, Object> getResultMapForPerson(ResultSet resultSet) throws SQLException {
        Map<String, Object> actualResults = new HashMap<>(PersonEntries.getPersonColumns().size());
        actualResults.put(PERSON_ID, resultSet.getInt(PERSON_ID));
        actualResults.put(PERSON_KEY, resultSet.getInt(PERSON_KEY));
        Blob blob = resultSet.getBlob(PERSON_VAL);
        Object deserializeVal = null;
        if (blob != null) {
            int length = (int) blob.length();
            deserializeVal = SERIALIZER.deserialize(ByteBuffer.wrap(blob.getBytes(1, length)));
        }
        actualResults.put(PERSON_VAL, deserializeVal);
        actualResults.put(PERSON_NAME, resultSet.getString(PERSON_NAME));
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
        return Stream.of(
                new FieldDescriptor(PERSON_ID_INDEX, PERSON_ID, INTEGER),
                new FieldDescriptor(PERSON_NAME_INDEX, PERSON_NAME, STRING),
                new FieldDescriptor(PERSON_VAL_INDEX, PERSON_VAL, new BlobValueTransformer(SERIALIZER)),
                new FieldDescriptor(PERSON_KEY_INDEX, PERSON_KEY, INTEGER)
        ).collect(Collectors.toMap(FieldDescriptor::getName, f -> f));
    }

    public static EntityDescriptor getPersonEntityDescriptor() {
        return new EntityDescriptor<>(Person.class, Person.PERSON_TABLE, PERSON_KEY, getPersonFieldDescriptor());
    }
}
