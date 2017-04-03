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
package com.epam.lathgertha.common;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public class CallableTask<R, V> {
    private final Scheduler scheduler;
    private final BiFunction<R, V, R> appender;

    private final AtomicReference<R> value = new AtomicReference<>(null);

    public CallableTask(Scheduler scheduler, BiFunction<R, V, R> appender) {
        this.scheduler = scheduler;
        this.appender = appender;
    }

    /**
     * Add value to response. Do not call this method from different threads.
     *
     * @param value to add to result.
     */
    public void append(V value) {
        this.value.set(appender.apply(this.value.getAndSet(null), value));
    }

    public R call(Runnable runnable) throws Exception {
        scheduler.pushTask(runnable);
        return value.getAndSet(null);
    }

    public R call() throws Exception {
        return value.getAndSet(null);
    }
}
