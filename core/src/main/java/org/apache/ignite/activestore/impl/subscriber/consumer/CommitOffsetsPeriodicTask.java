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

import org.apache.kafka.clients.consumer.Consumer;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/27/2017 1:00 PM
 */
class CommitOffsetsPeriodicTask implements Runnable {
    private final TransactionsBuffer buffer;
    private final OffsetCalculator offsetCalculator;
    private final Consumer<ByteBuffer, ByteBuffer> consumer;

    public CommitOffsetsPeriodicTask(
        TransactionsBuffer buffer,
        OffsetCalculator offsetCalculator,
        Consumer<ByteBuffer, ByteBuffer> consumer
    ) {
        this.buffer = buffer;
        this.offsetCalculator = offsetCalculator;
        this.consumer = consumer;
    }

    @Override public void run() {
       KafkaHelper.commitOffsets(buffer, offsetCalculator, consumer);
    }
}
