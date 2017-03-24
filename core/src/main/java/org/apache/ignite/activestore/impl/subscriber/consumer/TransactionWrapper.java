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
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Aleksandr_Meterko
 * @since 11/29/2016
 */
class TransactionWrapper {
    private final ByteBuffer value;
    private final ByteBuffer key;
    private final TopicPartition topicPartition;
    private final long offset;
    private final TransactionMetadata deserializedMetadata;

    public TransactionWrapper(ConsumerRecord<ByteBuffer, ByteBuffer> record, TransactionMetadata deserializedMetadata) {
        GridArgumentCheck.notNull(deserializedMetadata, "metadata cannot be null");
        this.value = record.value();
        this.key = record.key();
        this.topicPartition = new TopicPartition(record.topic(), record.partition());
        this.offset = record.offset();
        this.deserializedMetadata = deserializedMetadata;
    }

    public long id() {
        return deserializedMetadata.getTransactionId();
    }

    public ByteBuffer data() {
        return value;
    }

    public ByteBuffer metadata() {
        return key;
    }

    public TransactionMetadata deserializedMetadata() {
        return deserializedMetadata;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public long offset() {
        return offset;
    }

    @Override
    public String toString() {
        return "{" + deserializedMetadata.getTransactionId() + '}';
    }
}
