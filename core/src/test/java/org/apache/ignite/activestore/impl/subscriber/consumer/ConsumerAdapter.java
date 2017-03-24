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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Evgeniy_Ignatiev
 * @since 19:16 12/29/2016
 */
public class ConsumerAdapter<K, V> implements Consumer<K, V> {
    @Override public Set<TopicPartition> assignment() {
        return Collections.emptySet();
    }

    @Override public Set<String> subscription() {
        return Collections.emptySet();
    }

    @Override public void subscribe(Collection<String> topics) {
    }

    @Override public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    }

    @Override public void assign(Collection<TopicPartition> partitions) {
    }

    @Override public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    }

    @Override public void unsubscribe() {
    }

    @Override public ConsumerRecords<K, V> poll(long timeout) {
        return new ConsumerRecords<>(Collections.<TopicPartition, List<ConsumerRecord<K,V>>>emptyMap());
    }

    @Override public void commitSync() {
    }

    @Override public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override public void commitAsync() {
    }

    @Override public void commitAsync(OffsetCommitCallback callback) {
    }

    @Override public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    }

    @Override public void seek(TopicPartition partition, long offset) {
    }

    @Override public void seekToBeginning(Collection<TopicPartition> partitions) {
    }

    @Override public void seekToEnd(Collection<TopicPartition> partitions) {
    }

    @Override public long position(TopicPartition partition) {
        return 0;
    }


    @Override public OffsetAndMetadata committed(TopicPartition partition) {
        return null;
    }

    @Override public Map<MetricName, ? extends Metric> metrics() {
        return Collections.emptyMap();
    }

    @Override public List<PartitionInfo> partitionsFor(String topic) {
        return Collections.emptyList();
    }

    @Override public Map<String, List<PartitionInfo>> listTopics() {
        return Collections.emptyMap();
    }

    @Override public Set<TopicPartition> paused() {
        return Collections.emptySet();
    }

    @Override public void pause(Collection<TopicPartition> partitions) {
    }

    @Override public void resume(Collection<TopicPartition> partitions) {
    }

    @Override public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
        Map<TopicPartition, Long> timestampsToSearch) {
        return Collections.emptyMap();
    }

    @Override public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return Collections.emptyMap();
    }

    @Override public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return Collections.emptyMap();
    }

    @Override public void close() {
    }

    @Override public void wakeup() {
    }
}
