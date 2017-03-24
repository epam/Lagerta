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

package org.apache.ignite.activestore.impl.kafka;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import org.apache.ignite.activestore.commons.retry.Retries;
import org.apache.ignite.activestore.commons.retry.RetryRunnableAsyncOnCallback;
import org.apache.ignite.activestore.commons.retry.RetryStrategy;
import org.apache.ignite.lang.IgniteInClosure;
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
import org.apache.kafka.common.errors.RetriableException;

/**
 * @author Andrei_Yakushin
 * @since 12/19/2016 4:03 PM
 */
public class ConsumerProxyRetry<K, V> implements Consumer<K, V> {
    private static final Callable EMPTY_POLL = new Callable() {
        @Override
        public Object call() throws Exception {
            return new ConsumerRecords(Collections.<TopicPartition, List<ConsumerRecord>>emptyMap());
        }
    };

    private final Consumer<K, V> inner;
    private final Runnable onStop;

    public ConsumerProxyRetry(Consumer<K, V> inner, Runnable onStop) {
        this.inner = inner;
        this.onStop = onStop;
    }

    private RetryStrategy strategy() {
        return Retries
            .defaultStrategy(onStop)
            .retryException(RetriableException.class);
    }

    @Override
    public Set<TopicPartition> assignment() {
        return Retries.tryMe(new Callable<Set<TopicPartition>>() {
            @Override
            public Set<TopicPartition> call() throws Exception {
                return inner.assignment();
            }
        }, strategy());
    }

    @Override
    public Set<String> subscription() {
        return Retries.tryMe(new Callable<Set<String>>() {
            @Override
            public Set<String> call() throws Exception {
                return inner.subscription();
            }
        }, strategy());
    }

    @Override
    public void subscribe(final Collection<String> topics) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.subscribe(topics);
            }
        }, strategy());
    }

    @Override
    public void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.subscribe(topics, callback);
            }
        }, strategy());
    }

    @Override
    public void assign(final Collection<TopicPartition> partitions) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.assign(partitions);
            }
        }, strategy());
    }

    @Override
    public void subscribe(final Pattern pattern, final ConsumerRebalanceListener callback) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.subscribe(pattern, callback);
            }
        }, strategy());
    }

    @Override
    public void unsubscribe() {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.unsubscribe();
            }
        }, strategy());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ConsumerRecords<K, V> poll(final long timeout) {
        return Retries.tryMe(new Callable<ConsumerRecords<K, V>>() {
            @Override
            public ConsumerRecords<K, V> call() throws Exception {
                return inner.poll(timeout);
            }
        }, strategy(), EMPTY_POLL);
    }

    @Override
    public void commitSync() {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.commitSync();
            }
        }, strategy());
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.commitSync(offsets);
            }
        }, strategy());
    }

    @Override
    public void commitAsync() {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.commitAsync();
            }
        }, strategy());
    }

    @Override
    public void commitAsync(final OffsetCommitCallback callback) {
        Retries.tryMe(new IgniteInClosure<RetryRunnableAsyncOnCallback>() {
            @Override
            public void apply(final RetryRunnableAsyncOnCallback retryRunnableAsyncOnCallback) {
                inner.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        callback.onComplete(offsets, exception);
                        if (exception != null) {
                            retryRunnableAsyncOnCallback.retry(exception);
                        }
                    }
                });
            }
        }, strategy());
    }

    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        Retries.tryMe(new IgniteInClosure<RetryRunnableAsyncOnCallback>() {
            @Override
            public void apply(final RetryRunnableAsyncOnCallback retryRunnableAsyncOnCallback) {
                inner.commitAsync(offsets, new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        callback.onComplete(offsets, exception);
                        if (exception != null) {
                            retryRunnableAsyncOnCallback.retry(exception);
                        }
                    }
                });
            }
        }, strategy());
    }

    @Override
    public void seek(final TopicPartition partition, final long offset) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.seek(partition, offset);
            }
        }, strategy());
    }

    @Override
    public void seekToBeginning(final Collection<TopicPartition> partitions) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.seekToBeginning(partitions);
            }
        }, strategy());
    }

    @Override
    public void seekToEnd(final Collection<TopicPartition> partitions) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.seekToEnd(partitions);
            }
        }, strategy());
    }

    @Override
    public long position(final TopicPartition partition) {
        return Retries.tryMe(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return inner.position(partition);
            }
        }, strategy());
    }

    @Override
    public OffsetAndMetadata committed(final TopicPartition partition) {
        return Retries.tryMe(new Callable<OffsetAndMetadata>() {
            @Override
            public OffsetAndMetadata call() throws Exception {
                return inner.committed(partition);
            }
        }, strategy());
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Retries.tryMe(new Callable<Map<MetricName, ? extends Metric>>() {
            @Override
            public Map<MetricName, ? extends Metric> call() throws Exception {
                return inner.metrics();
            }
        }, strategy());
    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic) {
        return Retries.tryMe(new Callable<List<PartitionInfo>>() {
            @Override
            public List<PartitionInfo> call() throws Exception {
                return inner.partitionsFor(topic);
            }
        }, strategy());
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return Retries.tryMe(new Callable<Map<String, List<PartitionInfo>>>() {
            @Override
            public Map<String, List<PartitionInfo>> call() throws Exception {
                return inner.listTopics();
            }
        }, strategy());
    }

    @Override
    public Set<TopicPartition> paused() {
        return Retries.tryMe(new Callable<Set<TopicPartition>>() {
            @Override
            public Set<TopicPartition> call() throws Exception {
                return inner.paused();
            }
        }, strategy());
    }

    @Override
    public void pause(final Collection<TopicPartition> partitions) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.pause(partitions);
            }
        }, strategy());
    }

    @Override
    public void resume(final Collection<TopicPartition> partitions) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.resume(partitions);
            }
        }, strategy());
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            final Map<TopicPartition, Long> timestampsToSearch) {
        return Retries.tryMe(new Callable<Map<TopicPartition, OffsetAndTimestamp>>() {
            @Override
            public Map<TopicPartition, OffsetAndTimestamp> call() throws Exception {
                return inner.offsetsForTimes(timestampsToSearch);
            }
        }, strategy());
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions) {
        return Retries.tryMe(new Callable<Map<TopicPartition, Long>>() {
            @Override
            public Map<TopicPartition, Long> call() throws Exception {
                return inner.beginningOffsets(partitions);
            }
        }, strategy());
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
        return Retries.tryMe(new Callable<Map<TopicPartition, Long>>() {
            @Override
            public Map<TopicPartition, Long> call() throws Exception {
                return inner.endOffsets(partitions);
            }
        }, strategy());
    }

    @Override
    public void close() {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.close();
            }
        }, strategy());
    }

    @Override
    public void wakeup() {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.wakeup();
            }
        }, strategy());
    }
}
