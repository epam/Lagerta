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
import java.util.concurrent.Future;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * @author Andrei_Yakushin
 * @since 12/16/2016 10:04 AM
 */
public class Retries {
    private Retries() {
    }

    //------------------------------------------------------------------------------------------------------------------

    public static void tryMe(Runnable runnable, RetryStrategy strategy) {
        new RetryRunnable(runnable, strategy).run();
    }

    public static <V> V tryMe(Callable<V> callable, RetryStrategy strategy, Callable<V> defaultValue) {
        try {
            return new RetryCallable<>(callable, strategy, defaultValue).call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static <V> V tryMe(Callable<V> callable, RetryStrategy strategy) {
        return tryMe(callable, strategy, RetryCallable.<V>getNull());
    }

    public static void tryMe(IgniteInClosure<RetryRunnableAsyncOnCallback> closure, RetryStrategy strategy) {
        new RetryRunnableAsyncOnCallback(closure, strategy).run();
    }

    public static <V> Future<V> tryMe(IgniteClosure<RetryCallableAsyncOnCallback, Future<V>> inner, RetryStrategy retryStrategy) {
        try {
            return new RetryCallableAsyncOnCallback<>(inner, retryStrategy).call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    public static void tryMe(Runnable runnable) {
        tryMe(runnable, defaultStrategy());
    }

    public static <V> V tryMe(Callable<V> callable, Callable<V> defaultValue) {
        return tryMe(callable, defaultStrategy(), defaultValue);
    }

    public static <V> V tryMe(Callable<V> callable) {
        return tryMe(callable, defaultStrategy());
    }

    public static void tryMe(IgniteInClosure<RetryRunnableAsyncOnCallback> closure) {
        tryMe(closure, defaultStrategy());
    }

    public static <V> Future<V> tryMe(IgniteClosure<RetryCallableAsyncOnCallback, Future<V>> inner) {
        return tryMe(inner, defaultStrategy());
    }

    //------------------------------------------------------------------------------------------------------------------

    public static void tryMe(Runnable runnable, Runnable onStop) {
        tryMe(runnable, defaultStrategy(onStop));
    }

    public static <V> V tryMe(Callable<V> callable, Runnable onStop, Callable<V> defaultValue) {
        return tryMe(callable, defaultStrategy(onStop), defaultValue);
    }

    public static <V> V tryMe(Callable<V> callable, Runnable onStop) {
        return tryMe(callable, defaultStrategy(onStop));
    }

    public static void tryMe(IgniteInClosure<RetryRunnableAsyncOnCallback> closure, Runnable onStop) {
        tryMe(closure, defaultStrategy(onStop));
    }

    public static <V> Future<V> tryMe(IgniteClosure<RetryCallableAsyncOnCallback, Future<V>> inner, Runnable onStop) {
        return tryMe(inner, defaultStrategy(onStop));
    }

    //------------------------------------------------------------------------------------------------------------------

    public static RetryStrategy defaultStrategy(Runnable onStop) {
        return defaultStrategy()
            .onStop(onStop);
    }

    public static RetryStrategy defaultStrategy() {
        return new RetryStrategy()
            .delay(1)
            .backoff(1000)
            .retries(60);
    }
}
