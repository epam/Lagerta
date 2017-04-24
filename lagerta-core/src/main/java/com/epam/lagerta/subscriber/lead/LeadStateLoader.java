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

package com.epam.lagerta.subscriber.lead;

import com.epam.lagerta.kafka.KafkaFactory;
import com.epam.lagerta.kafka.SubscriberConfig;
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
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.function.Function.identity;

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
        Stream<TopicPartition> partitionStream;
        try (Consumer<?, ?> consumer = createConsumer()) {
            shiftToLastCommitted(consumer, commitId);
            partitionStream = getTopicPartitionStream(consumer);
        }
        List<ConsumerKeeper> consumerKeepers = partitionStream
                .map(tp -> new ConsumerKeeper(createAndSubscribeConsumer()))
                .peek(consumerKeeper -> consumerKeeper.consumer().poll(0))
                .collect(Collectors.toList());
        CommittedTransactions committed = new CommittedTransactions();
        ForkJoinPool pool = new ForkJoinPool(consumerKeepers.size());
        pool.submit(() -> {
            while (true) {
                List<List<List<Long>>> collect = consumerKeepers
                        .parallelStream()
                        .filter(ConsumerKeeper::isAlive)
                        .map(this::consumePartitionUntilOffset)
                        .collect(Collectors.toList());
                collect.stream().flatMap(Collection::stream).forEach(committed::addAll);
                committed.compress();
                if (collect.isEmpty()) {
                    break;
                }
            }
            return committed;
        }).join();
        consumerKeepers.forEach(ConsumerKeeper::close);
        return committed;
    }

    private List<List<Long>> consumePartitionUntilOffset(ConsumerKeeper consumerKeeper) {
        List<List<Long>> mainBuffer = new ArrayList<>(PAGE_SIZE);
        for (int i = 0; i < PAGE_SIZE; i++) {
            ConsumerRecords<?, ?> records = consumerKeeper.consumer().poll(POLLING_TIME);
            List<Long> recordBuffer = recordStream(records)
                    .map(ConsumerRecord::timestamp)
                    .collect(Collectors.toList());
            recordBuffer.sort(Long::compareTo);
            mainBuffer.add(recordBuffer);
            if (consumerKeeper.isAlive(records)) {
                break;
            }
        }
        return mainBuffer;
    }

    private Consumer<?, ?> createAndSubscribeConsumer() {
        Consumer<?, ?> consumer = createConsumer();
        consumer.subscribe(Collections.singleton(config.getRemoteTopic()));
        return consumer;
    }

    private Consumer<?, ?> createConsumer() {
        Properties properties = config.getConsumerConfig();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return kafkaFactory.consumer(properties);
    }

    private void shiftToLastCommitted(Consumer<?, ?> consumer, long commitId) {
        Map<TopicPartition, Long> partitionsAndTimestamps = getTopicPartitionStream(consumer)
                .collect(Collectors.toMap(identity(), v -> commitId));
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

    private static <K, V> Stream<ConsumerRecord<K, V>> recordStream(ConsumerRecords<K, V> records) {
        return StreamSupport.stream(records.spliterator(), false);
    }

    private static class ConsumerKeeper {
        private final Consumer<?, ?> consumer;
        private Long endOffset;
        private boolean alive = true;

        ConsumerKeeper(Consumer<?, ?> consumer) {
            this.consumer = consumer;
        }

        Consumer<?, ?> consumer() {
            return consumer;
        }

        boolean isAlive() {
            return alive;
        }

        boolean isAlive(ConsumerRecords<?, ?> records) {
            if (endOffset == null) {
                endOffset = recordStream(records)
                        .findFirst()
                        .map(record -> new TopicPartition(record.topic(), record.partition()))
                        .map(tp -> consumer.endOffsets(Collections.singletonList(tp)).get(tp))
                        .orElse(null);
            }
            alive = endOffset != null && recordStream(records)
                    .map(ConsumerRecord::offset)
                    .max(Long::compareTo)
                    .map(f -> f >= endOffset)
                    .orElse(false);
            return alive;
        }

        void close() {
            consumer.close();
        }
    }
}
