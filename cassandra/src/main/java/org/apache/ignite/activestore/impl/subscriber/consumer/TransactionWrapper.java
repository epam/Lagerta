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

package org.apache.ignite.activestore.impl.subscriber.consumer;

import java.nio.ByteBuffer;
import org.apache.ignite.activestore.impl.kv.MessageMetadata;
import org.apache.ignite.internal.util.GridArgumentCheck;

/**
 * @author Aleksandr_Meterko
 * @since 11/29/2016
 */
public class TransactionWrapper {

    private final MessageMetadata metadata;
    private final ByteBuffer data;

    public TransactionWrapper(MessageMetadata metadata, ByteBuffer data) {
        GridArgumentCheck.notNull(metadata, "metadata cannot be null");
        this.metadata = metadata;
        this.data = data;
    }

    public MessageMetadata metadata() {
        return metadata;
    }

    public ByteBuffer data() {
        return data;
    }

}
