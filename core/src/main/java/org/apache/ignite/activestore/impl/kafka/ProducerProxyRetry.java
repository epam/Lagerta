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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.activestore.commons.retry.Retries;
import org.apache.ignite.activestore.commons.retry.RetryCallableAsyncOnCallback;
import org.apache.ignite.activestore.commons.retry.RetryStrategy;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.RetriableException;

/**
 * @author Andrei_Yakushin
 * @since 12/19/2016 3:55 PM
 */
public class ProducerProxyRetry<K, V> implements Producer<K, V> {
    private final Producer<K, V> inner;
    private final Runnable onStop;

    public ProducerProxyRetry(Producer<K, V> inner, Runnable onStop) {
        this.inner = inner;
        this.onStop = onStop;
    }

    private RetryStrategy strategy() {
        return Retries
            .defaultStrategy(onStop)
            .retryException(RetriableException.class);
    }

    @Override
    public Future<RecordMetadata> send(final ProducerRecord<K, V> record) {
        return Retries.tryMe(new Callable<Future<RecordMetadata>>() {
            @Override
            public Future<RecordMetadata> call() throws Exception {
                return inner.send(record);
            }
        }, strategy());
    }

    @Override
    public Future<RecordMetadata> send(final ProducerRecord<K, V> record, final Callback callback) {
        return Retries.tryMe(new IgniteClosure<RetryCallableAsyncOnCallback, Future<RecordMetadata>>() {
            @Override
            public Future<RecordMetadata> apply(final RetryCallableAsyncOnCallback retryCallableAsyncOnCallback) {
                return inner.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        callback.onCompletion(metadata, exception);
                        if (exception != null) {
                            retryCallableAsyncOnCallback.retry(exception);
                        }
                    }
                });
            }
        });
    }

    @Override
    public void flush() {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                inner.flush();
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
    public Map<MetricName, ? extends Metric> metrics() {
        return Retries.tryMe(new Callable<Map<MetricName, ? extends Metric>>() {
            @Override
            public Map<MetricName, ? extends Metric> call() throws Exception {
                return inner.metrics();
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
    public void close(final long timeout, final TimeUnit unit) {
        Retries.tryMe(onStop, new Runnable() {
            @Override
            public void run() {
                inner.close(timeout, unit);
            }
        });
    }
}
