/*
 * Copyright 2017 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.lagerta.base;

import com.epam.lagerta.util.Serializer;

import javax.sql.rowset.serial.SerialBlob;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BlobValueTransformer implements ValueTransformer {
    private final Serializer serializer;

    public BlobValueTransformer(Serializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public Object get(ResultSet resultSet, int index) throws SQLException {
        Blob blob = resultSet.getBlob(index);
        if (blob != null) {
            ByteBuffer wrap = ByteBuffer.wrap(blob.getBytes(0, (int) blob.length()));
            return serializer.deserialize(wrap);
        }
        return null;
    }

    @Override
    public void set(PreparedStatement preparedStatement, int index, Object value) throws SQLException {
        preparedStatement.setBlob(index, new SerialBlob(serializer.serialize(value).array()));
    }

    @Override
    public String toString() {
        return "BLOB";
    }
}
