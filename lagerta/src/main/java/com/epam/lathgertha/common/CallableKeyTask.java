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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CallableKeyTask<R, K, V> {
    private final Scheduler scheduler;
    private final TripleFunction<R, K, V, R> appender;

    private final Map<K, R> value = new ConcurrentHashMap<>();

    public CallableKeyTask(Scheduler scheduler, TripleFunction<R, K, V, R> appender) {
        this.scheduler = scheduler;
        this.appender = appender;
    }

    /**
     * Add value to response. Do not call this method from different threads.
     *
     * @param key add <tt>value</tt> by the <tt>key</tt>
     * @param value to add to result by <tt>key</tt>
     */
    public void append(K key, V value) {
        this.value.put(key, appender.apply(this.value.remove(key), key, value));
    }

    public R call(K key, Runnable runnable) {
        scheduler.pushTask(runnable);
        return value.remove(key);
    }
}
