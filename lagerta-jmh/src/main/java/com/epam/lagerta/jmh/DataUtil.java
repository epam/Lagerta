package com.epam.lagerta.jmh;

import com.epam.lagerta.capturer.TransactionScope;
import com.google.common.collect.Lists;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

final class DataUtil {

    static List<TransactionScope> get10Transactions(String cacheName, long from, Object... keys) {
        List<TransactionScope> list = list();
        for (long i = from; i < from + 10L; i++) {
            list.add(txScope(i, cacheScope(cacheName, keys)));
        }
        return list;
    }

    static List<TransactionScope> get1000Transactions(String cacheName, long from, Object... keys) {
        List<TransactionScope> list = list();
        for (long i = from; i < from + 1000L; i++) {
            list.add(txScope(i, cacheScope(cacheName, keys)));
        }
        return list;
    }

    static List<Long> getCommitRange(long from, long to) {
        List<Long> list = list();
        for (long i = from; i < to; i += 4) {
            list.add(i);
        }
        return list;
    }

    @SafeVarargs
    static TransactionScope txScope(long txId, Map.Entry<String, List>... cacheScopes) {
        return new TransactionScope(txId, Lists.newArrayList(cacheScopes));
    }

    static Map.Entry<String, List> cacheScope(String cacheName, Object... keys) {
        return new AbstractMap.SimpleImmutableEntry<>(cacheName, Lists.newArrayList(keys));
    }

    @SafeVarargs
    static <T> List<T> list(T... ts) {
        return Lists.newArrayList(ts);
    }
}
