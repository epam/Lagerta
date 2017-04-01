package com.epam.lathgertha.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Andrei_Yakushin
 * @since 01.04.2017 7:59
 */
public class CallableKeyTask<V, K, T> {
    private final Scheduler scheduler;
    private final TripleFunction<V, K, T, V> appender;

    private final Map<K, V> value = new ConcurrentHashMap<>();

    public CallableKeyTask(Scheduler scheduler, TripleFunction<V, K, T, V> appender) {
        this.scheduler = scheduler;
        this.appender = appender;
    }

    public void append(K key, T value) {
        V old = this.value.remove(key);
        this.value.put(key, appender.apply(old, key, value));
    }

    public V call(K key, Runnable runnable) throws Exception {
        scheduler.pushTask(runnable);
        return value.get(key);
    }
}
