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

package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.capturer.TransactionScope;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;

public class TransactionData {

    private final TransactionScope transactionScope;
    private final ByteBuffer value;
    private final TopicPartition topicPartition;
    private final Long offset;

    public TransactionData(TransactionScope transactionScope, ByteBuffer value, TopicPartition topicPartition, Long offset) {
        this.transactionScope = transactionScope;
        this.value = value;
        this.topicPartition = topicPartition;
        this.offset = offset;
    }

    public TransactionScope getTransactionScope() {
        return transactionScope;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public Long getOffset() {
        return offset;
    }
}
