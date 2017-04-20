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

package com.epam.lagerta.jmh.utils;

import com.epam.lagerta.capturer.TransactionScope;
import com.google.common.collect.Lists;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public final class DataUtil {

    static final String CACHE_NAME = "cache";
    private static final long COMMON_KEY = 0L;

    public static List<TransactionScope> getIndependentTransactions(int count, List<Long> pattern) {
        return getTransactionScopes(count, pattern, DataUtil::list);
    }

    public static List<TransactionScope> getDependentTransactions(int count, List<Long> pattern) {
        TransactionGenerator txGenerator = id -> id % 2 == 0 ? list(id, id + 1) : list(id);
        return getTransactionScopes(count, pattern, txGenerator);
    }

    public static List<TransactionScope> getTotalDependentTransactions(int count, List<Long> pattern) {
        return getTransactionScopes(count, pattern, id -> list(COMMON_KEY));
    }

    private static List<TransactionScope> getTransactionScopes(int count, List<Long> pattern,
                                                               TransactionGenerator txGenerator) {
        IdGenerator idGenerator = new IdGenerator(pattern);
        return IntStream
                .range(0, count)
                .mapToObj(i -> txGenerator.generate(idGenerator.getNextId()))
                .collect(Collectors.toList());
    }

    public static Stream<Long> getCommitRange(long from, long to, LongPredicate predicate) {
        return LongStream
                .range(from, to)
                .filter(predicate)
                .boxed();
    }

    @SafeVarargs
    static TransactionScope txScope(long txId, Map.Entry<String, List>... cacheScopes) {
        return new TransactionScope(txId, Lists.newArrayList(cacheScopes));
    }

    static Map.Entry<String, List> cacheScope(String cacheName, Object... keys) {
        return new AbstractMap.SimpleImmutableEntry<>(cacheName, Lists.newArrayList(keys));
    }

    @SafeVarargs
    public static <T> List<T> list(T... ts) {
        return Lists.newArrayList(ts);
    }
}
