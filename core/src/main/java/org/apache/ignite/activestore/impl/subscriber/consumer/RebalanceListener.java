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
import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/23/2016 7:05 PM
 */
class RebalanceListener implements ConsumerRebalanceListener {
    private final TransactionsBuffer buffer;
    private final OffsetCalculator offsetCalculator;
    private final Consumer<ByteBuffer, ByteBuffer> consumer;

    RebalanceListener(TransactionsBuffer buffer, OffsetCalculator offsetCalculator,
        Consumer<ByteBuffer, ByteBuffer> consumer) {
        this.buffer = buffer;
        this.offsetCalculator = offsetCalculator;
        this.consumer = consumer;
    }

    @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        KafkaHelper.commitOffsets(buffer, offsetCalculator, consumer);
    }

    @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Do nothing.
    }
}
