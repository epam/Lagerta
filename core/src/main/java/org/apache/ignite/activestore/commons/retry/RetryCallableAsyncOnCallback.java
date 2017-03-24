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

package org.apache.ignite.activestore.commons.retry;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import org.apache.ignite.lang.IgniteClosure;

/**
 * @author Andrei_Yakushin
 * @since 12/20/2016 12:33 PM
 */
public class RetryCallableAsyncOnCallback<V> implements Callable<Future<V>> {
    private final IgniteClosure<RetryCallableAsyncOnCallback, Future<V>> inner;
    private final RetryStrategy retryStrategy;

    private final Queue<Future<V>> queue = new ConcurrentLinkedQueue<>();

    public RetryCallableAsyncOnCallback(IgniteClosure<RetryCallableAsyncOnCallback, Future<V>> inner,
        RetryStrategy retryStrategy) {
        this.inner = inner;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public Future<V> call() throws Exception {
        queue.add(inner.apply(this));
        return new RetryFuture<>(queue);
    }

    public void retry(Exception exception) {
        if (retryStrategy.isRetryPossible(exception)) {
            queue.add(inner.apply(this));
        }
    }
}
