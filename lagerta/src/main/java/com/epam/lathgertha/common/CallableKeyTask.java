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
        this.value.put(key, appender.apply(this.value.remove(key), key, value));
    }

    public V call(K key, Runnable runnable) {
        scheduler.pushTask(runnable);
        return value.remove(key);
    }
}
