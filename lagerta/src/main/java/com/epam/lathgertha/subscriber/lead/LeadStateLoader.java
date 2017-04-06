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

package com.epam.lathgertha.subscriber.lead;

import com.epam.lathgertha.kafka.KafkaFactory;
import com.epam.lathgertha.kafka.SubscriberConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LeadStateLoader {

    private static final long POLLING_TIME = 100L;
    private static final int PAGE_SIZE = 4;

    private final KafkaFactory kafkaFactory;
    private final SubscriberConfig config;
    private final String groupId;

    public LeadStateLoader(KafkaFactory kafkaFactory, SubscriberConfig config, String groupId) {
        this.kafkaFactory = kafkaFactory;
        this.config = config;
        this.groupId = groupId + UUID.randomUUID();
    }

    public CommittedTransactions loadCommitsAfter(long commitId) {
        Consumer<?, ?> consumer = createConsumer();
        Map<TopicPartition, Long> ends = getPartitionOffsets(consumer);
        shiftToLastCommitted(consumer, commitId);
        CommittedTransactions committed = new CommittedTransactions();
        List<StateKeeper> stateKeepers = ends.entrySet()
                .stream()
                .map(entry -> new StateKeeper(createConsumer(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
        while (true) {
            List<List<List<Long>>> collect = stateKeepers
                    .parallelStream()
                    .filter(state -> state.alive)
                    .map(this::consumePartitionUntilOffset)
                    .collect(Collectors.toList());
            if (collect.isEmpty()) {
                break;
            }
            collect.stream().flatMap(Collection::stream).forEach(committed::addAll);
            committed.compress();
        }
        return committed;
    }

    private List<List<Long>> consumePartitionUntilOffset(StateKeeper stateKeeper) {
        List<List<Long>> mainBuffer = new ArrayList<>(PAGE_SIZE);
        for (int i = 0; i < PAGE_SIZE; i++) {
            ConsumerRecords<?, ?> records = stateKeeper.consumer.poll(POLLING_TIME);
            List<Long> recordBuffer = StreamSupport.stream(records.spliterator(), false)
                    .map(ConsumerRecord::timestamp)
                    .collect(Collectors.toList());
            recordBuffer.sort(Long::compareTo);
            mainBuffer.add(recordBuffer);
            Boolean cond = StreamSupport.stream(records.spliterator(), false)
                    .map(ConsumerRecord::offset)
                    .max(Long::compareTo)
                    .map(f -> f >= stateKeeper.endOffset)
                    .orElse(false);
            if (cond) {
                stateKeeper.alive = false;
                break;
            }
        }
        return mainBuffer;
    }

    private Consumer<?, ?> createConsumer(TopicPartition partition) {
        Consumer<?, ?> consumer = createConsumer();
        consumer.assign(Collections.singletonList(partition));
        return consumer;
    }

    private Consumer<?, ?> createConsumer() {
        Properties properties = config.getConsumerConfig();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return kafkaFactory.consumer(properties);
    }

    private Map<TopicPartition, Long> getPartitionOffsets(Consumer<?, ?> consumer) {
        Stream<TopicPartition> topicPartitionStream = getTopicPartitionStream(consumer);
        return consumer.endOffsets(topicPartitionStream.collect(Collectors.toList()));
    }

    private void shiftToLastCommitted(Consumer<?, ?> consumer, long commitId) {
        Map<TopicPartition, Long> partitionsAndTimestamps = getTopicPartitionStream(consumer)
                .collect(Collectors.toMap(k -> k, v -> commitId));
        Map<TopicPartition, OffsetAndMetadata> partitionsAndOffsets = consumer
                .offsetsForTimes(partitionsAndTimestamps)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, v -> new OffsetAndMetadata(v.getValue().offset())));
        consumer.commitSync(partitionsAndOffsets);
    }

    private Stream<TopicPartition> getTopicPartitionStream(Consumer<?, ?> consumer) {
        String topic = config.getRemoteTopic();
        return consumer.partitionsFor(topic).stream()
                .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()));
    }

    private static class StateKeeper {
        final Consumer<?, ?> consumer;
        final Long endOffset;
        boolean alive = true;

        private StateKeeper(Consumer<?, ?> consumer, Long endOffset) {
            this.consumer = consumer;
            this.endOffset = endOffset;
        }
    }
}
