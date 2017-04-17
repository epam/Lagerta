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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

public final class AtomicsHelper {
    private static final String NAME = "ACTIVE_CACHE_STORE_ATOMICS";

    private AtomicsHelper() {
    }

    public static CacheConfiguration<String, Object> getConfig() {
        return new CacheConfiguration<String, Object>()
                .setName(NAME)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setCacheMode(CacheMode.REPLICATED);
    }

    public static <V> Atomic<V> getAtomic(Ignite ignite, String name) {
        return new CacheAtomic<>(ignite.cache(NAME), name);
    }

    private static class CacheAtomic<V> implements Atomic<V> {
        private final IgniteCache<String, V> cache;
        private final String name;

        CacheAtomic(IgniteCache<String, V> cache, String name) {
            this.cache = cache;
            this.name = name;
        }

        @Override
        public V get() {
            return cache == null ? null : cache.get(name);
        }

        @Override
        public void set(V value) {
            if (cache != null) {
                if (value == null) {
                    cache.remove(name);
                } else {
                    cache.put(name, value);
                }
            }
        }

        @Override
        public V initIfAbsent(V value) {
            if (cache != null) {
                cache.putIfAbsent(name, value);
                return get();
            } else {
                return null;
            }
        }
    }
}
