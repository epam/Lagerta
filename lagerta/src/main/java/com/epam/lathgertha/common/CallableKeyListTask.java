package com.epam.lathgertha.common;

import java.util.List;

/**
 * @author Andrei_Yakushin
 * @since 01.04.2017 13:31
 */
public class CallableKeyListTask<K, E> extends CallableKeyTask<List<E>, K, List<E>> {
    public CallableKeyListTask(Scheduler scheduler) {
        super(scheduler, (old, k, toAppend) -> {
            if (old != null) {
                toAppend.addAll(old);
            }
            return toAppend;
        });
    }
}
