package com.epam.lathgertha.common;

import java.util.function.BiFunction;

/**
 * @author Andrei_Yakushin
 * @since 01.04.2017 7:43
 */
public class CallableTask<V, T> {
    private final Scheduler scheduler;
    private final BiFunction<V, T, V> appender;

    private volatile V value;

    public CallableTask(Scheduler scheduler, BiFunction<V, T, V> appender) {
        this.scheduler = scheduler;
        this.appender = appender;
    }

    public void append(T value) {
        this.value = appender.apply(this.value, value);
    }

    public V call(Runnable runnable) throws Exception {
        scheduler.pushTask(runnable);
        return value;
    }
}
