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

import java.util.concurrent.Callable;

/**
 * @author Andrei_Yakushin
 * @since 12/16/2016 9:51 AM
 */
public class RetryCallable<V> implements Callable<V> {
    public static final Callable NULL = new Callable() {
        @Override
        public Object call() throws Exception {
            return null;
        }
    };

    private final Callable<V> inner;
    private final RetryStrategy retryStrategy;
    private final Callable<V> defaultValue;

    public RetryCallable(Callable<V> inner, RetryStrategy retryStrategy, Callable<V> defaultValue) {
        this.inner = inner;
        this.retryStrategy = retryStrategy;
        this.defaultValue = defaultValue;
    }

    @Override
    public V call() throws Exception {
        Exception exception;
        do {
            try {
                return inner.call();
            } catch (Exception e) {
                exception = e;
            }
        } while (retryStrategy.isRetryPossible(exception));
        return defaultValue.call();
    }

    public static <V> Callable<V> getNull() {
        return (Callable<V>) NULL;
    }
}
