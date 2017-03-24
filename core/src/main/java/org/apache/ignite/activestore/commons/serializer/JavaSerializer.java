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

package org.apache.ignite.activestore.commons.serializer;

import java.nio.ByteBuffer;
import org.springframework.util.SerializationUtils;

/**
 * @author Aleksandr_Meterko
 * @since 1/24/2017
 */
public class JavaSerializer implements Serializer {

    @Override public <T> ByteBuffer serialize(T obj) {
        byte[] serialized = SerializationUtils.serialize(obj);
        return serialized != null ? ByteBuffer.wrap(serialized) : null;
    }

    @SuppressWarnings("unchecked")
    @Override public <T> T deserialize(ByteBuffer buffer) {
        return (T)SerializationUtils.deserialize(buffer.array());
    }
}
