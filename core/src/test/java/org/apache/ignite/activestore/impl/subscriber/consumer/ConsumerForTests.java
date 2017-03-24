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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
 * @since 19:15 12/29/2016
 */
public class ConsumerForTests<K, V, C extends Consumer<K, V>> extends ConsumerAdapter<K, V> {
    private final C delegate;

    private volatile ConsumerAdapter<K, V> substitute;

    public ConsumerForTests(C delegate) {
        this.delegate = delegate;
    }

    public C getDelegate() {
        return delegate;
    }

    public void setSubstitute(ConsumerAdapter<K, V> substitute) {
        this.substitute = substitute;
    }

    @Override public Set<TopicPartition> assignment() {
        return activeConsumer().assignment();
    }

    @Override public Set<String> subscription() {
        return activeConsumer().subscription();
    }

    @Override public void subscribe(Collection<String> topics) {
        activeConsumer().subscribe(topics);
    }

    @Override public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        activeConsumer().subscribe(topics, callback);
    }

    @Override public void assign(Collection<TopicPartition> partitions) {
        activeConsumer().assign(partitions);
    }

    @Override public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        activeConsumer().subscribe(pattern, callback);
    }

    @Override public void unsubscribe() {
        activeConsumer().unsubscribe();
    }

    @Override public ConsumerRecords<K, V> poll(long timeout) {
        return activeConsumer().poll(timeout);
    }

    @Override public void commitSync() {
        activeConsumer().commitSync();
    }

    @Override public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        activeConsumer().commitSync(offsets);
    }

    @Override public void commitAsync() {
        activeConsumer().commitAsync();
    }

    @Override public void commitAsync(OffsetCommitCallback callback) {
        activeConsumer().commitAsync(callback);
    }

    @Override public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        activeConsumer().commitAsync(offsets, callback);
    }

    @Override public void seek(TopicPartition partition, long offset) {
        activeConsumer().seek(partition, offset);
    }

    @Override public void seekToBeginning(Collection<TopicPartition> partitions) {
        activeConsumer().seekToBeginning(partitions);
    }

    @Override public void seekToEnd(Collection<TopicPartition> partitions) {
        activeConsumer().seekToEnd(partitions);
    }

    @Override public long position(TopicPartition partition) {
        return activeConsumer().position(partition);
    }


    @Override public OffsetAndMetadata committed(TopicPartition partition) {
        return activeConsumer().committed(partition);
    }

    @Override public Map<MetricName, ? extends Metric> metrics() {
        return activeConsumer().metrics();
    }

    @Override public List<PartitionInfo> partitionsFor(String topic) {
        return activeConsumer().partitionsFor(topic);
    }

    @Override public Map<String, List<PartitionInfo>> listTopics() {
        return activeConsumer().listTopics();
    }

    @Override public Set<TopicPartition> paused() {
        return activeConsumer().paused();
    }

    @Override public void pause(Collection<TopicPartition> partitions) {
        activeConsumer().pause(partitions);
    }

    @Override public void resume(Collection<TopicPartition> partitions) {
        activeConsumer().resume(partitions);
    }

    @Override public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
        Map<TopicPartition, Long> timestampsToSearch) {
        return activeConsumer().offsetsForTimes(timestampsToSearch);
    }

    @Override public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return activeConsumer().beginningOffsets(partitions);
    }

    @Override public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return activeConsumer().endOffsets(partitions);
    }

    @Override public void close() {
        if (substitute != null) {
            substitute.close();
        }
        delegate.close();
    }

    @Override public void wakeup() {
        activeConsumer().wakeup();
    }

    private Consumer<K, V> activeConsumer() {
        return substitute == null? delegate: substitute;
    }
}
