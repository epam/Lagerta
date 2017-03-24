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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Andrei_Yakushin
 * @since 12/20/2016 11:54 AM
 */
public class RetryFuture<V> implements Future<V> {
    private final Queue<Future<V>> queue;

    public RetryFuture(Queue<Future<V>> queue) {
        this.queue = queue;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancel = false;
        for (Future<V> future : queue) {
            cancel = cancel || future.cancel(mayInterruptIfRunning);
        }
        return cancel;
    }

    @Override
    public boolean isCancelled() {
        for (Future<V> future : queue) {
            if (!future.isCancelled()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isDone() {
        for (Future<V> future : queue) {
            if (!future.isDone()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        V result = null;
        for (Future<V> future : queue) {
            result = future.get();
        }
        return result;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        V result = null;
        for (Future<V> future : queue) {
            result = future.get(timeout, unit);
        }
        return result;
    }
}
