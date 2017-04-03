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

public class CallableTask<V, T> {
    private final Scheduler scheduler;
    private final BiFunction<V, T, V> appender;

    private final AtomicReference<V> value = new AtomicReference<>(null);

    public CallableTask(Scheduler scheduler, BiFunction<V, T, V> appender) {
        this.scheduler = scheduler;
        this.appender = appender;
    }

    public void append(T value) {
        this.value.set(appender.apply(this.value.get(), value));
    }

    public V call(Runnable runnable) throws Exception {
        scheduler.pushTask(runnable);
        return value.getAndSet(null);
    }
}
