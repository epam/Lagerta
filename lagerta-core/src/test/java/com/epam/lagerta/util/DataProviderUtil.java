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

package com.epam.lagerta.util;

import com.epam.lagerta.capturer.TransactionScope;
import com.google.common.collect.Lists;
import org.springframework.util.Assert;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataProviderUtil {
    public static Object[][] concat(Object[][] first, Object[][] second){
        Assert.isTrue(first.length == second.length, "Concatenation of providers of different size.");

        Object[][] concatenatedData = new Object[first.length][];

        for(int i = 0; i < first.length; i++){
            concatenatedData[i] = concat(first[i], second[i]);
        }
        return concatenatedData;
    }

    private static Object[] concat(Object[] first, Object[] second) {
        return Stream
                .concat(Arrays.stream(first), Arrays.stream(second))
                .toArray();
    }

    @SafeVarargs
    public static TransactionScope txScope(long txId, Map.Entry<String, List>... cacheScopes) {
        return new TransactionScope(txId, Lists.newArrayList(cacheScopes));
    }

    public static Map.Entry<String, List> cacheScope(String cacheName, Object... keys) {
        return new AbstractMap.SimpleImmutableEntry<>(cacheName, Lists.newArrayList(keys));
    }

    @SafeVarargs
    public static <T> List<T> list(T... ts) {
        return Lists.newArrayList(ts);
    }

    public static class NodeTransactionsBuilder {

        private Map<UUID, List<Long>> map;

        private NodeTransactionsBuilder(Map<UUID, List<Long>> map) {
            this.map = map;
        }

        public static NodeTransactionsBuilder builder() {
            return new NodeTransactionsBuilder(new HashMap<>());
        }

        public Map<UUID, List<Long>> build() {
            return map;
        }

        public NodeTransactionsBuilder nodeTransactions(UUID uuid, long... txIds) {
            List<Long> txs = Arrays.stream(txIds).boxed().collect(Collectors.toList());
            map.put(uuid, txs);
            return this;
        }
    }
}



