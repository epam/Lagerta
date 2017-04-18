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
package com.epam.lagerta.util;

import org.springframework.util.SerializationUtils;

import java.nio.ByteBuffer;

public class SerializerImpl implements Serializer {
    @Override
    public <T> ByteBuffer serialize(T obj) {
        return obj == null ? null : ByteBuffer.wrap(SerializationUtils.serialize(obj));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T deserialize(ByteBuffer buffer) {
        return buffer == null ? null : (T)SerializationUtils.deserialize(buffer.array());
    }
}