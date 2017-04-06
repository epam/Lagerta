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

package com.epam.lathgertha.subscriber;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

public class ReaderRebalanceListener implements ConsumerRebalanceListener {

    private final Consumer<ByteBuffer, ByteBuffer> consumer;
    private final Map<TopicPartition, CommittedOffset> committedOffsetMap;

    public ReaderRebalanceListener(Consumer<ByteBuffer, ByteBuffer> consumer,
                                   Map<TopicPartition, CommittedOffset> committedOffsetMap) {
        this.consumer = consumer;
        this.committedOffsetMap = committedOffsetMap;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        Reader.commitOffsets(consumer, committedOffsetMap);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        collection.forEach(committedOffsetMap::remove);
    }
}
