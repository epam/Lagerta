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
import com.epam.lathgertha.subscriber.util.MergeUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LeadStateLoader {

    private static final long POLLING_TIME = 100L;
    private static final long INITIAL_OFFSET = -1L;

    private final KafkaFactory kafkaFactory;
    private final SubscriberConfig config;
    private final String groupId;

    public LeadStateLoader(KafkaFactory kafkaFactory, SubscriberConfig config, String groupId) {
        this.kafkaFactory = kafkaFactory;
        this.config = config;
        this.groupId = groupId;
    }

    public List<Long> loadCommitsAfter(long commitId) {
        Consumer<?, ?> consumer = createConsumer();
        Map<TopicPartition, Long> beginnings = getPartitionOffsets(consumer);
        shiftToLastCommitted(consumer, commitId);
        return beginnings.entrySet()
                .parallelStream()
                .map(entry -> consumePartitionUntilOffset(entry.getKey(), entry.getValue()))
                .reduce(new LinkedList<>(), (l1, l2) -> {
                    MergeUtil.merge(l1, l2, Long::compareTo);
                    return l1;
                });
    }

    private List<Long> consumePartitionUntilOffset(TopicPartition partition, Long endOffset) {
        Consumer<?, ?> consumer = createConsumer();
        consumer.assign(Collections.singletonList(partition));
        List<Long> mainBuffer = new LinkedList<>();
        long lastPollOffset = INITIAL_OFFSET;
        do {
            List<Long> recordBuffer = new ArrayList<>();
            ConsumerRecords<?, ?> records = consumer.poll(POLLING_TIME);
            for (ConsumerRecord<?, ?> record : records) {
                recordBuffer.add(record.timestamp());
                long offset = record.offset();
                if (offset > lastPollOffset) {
                    lastPollOffset = offset;
                }
            }
            merge(mainBuffer, recordBuffer);
        } while (lastPollOffset < endOffset && lastPollOffset != INITIAL_OFFSET);
        return mainBuffer;
    }

    private void merge(List<Long> container, List<Long> buffer) {
        buffer.sort(Long::compareTo);
        MergeUtil.merge(container, buffer, Long::compareTo);
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
}
