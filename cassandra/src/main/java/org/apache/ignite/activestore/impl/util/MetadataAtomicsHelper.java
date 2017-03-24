/*
 * Copyright (c) 2017. EPAM Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.activestore.impl.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.activestore.commons.Reference;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * @author Andrei_Yakushin
 * @since 10/12/2016 10:39 AM
 */
public class MetadataAtomicsHelper {
    private static final String NAME = "ACTIVE_CACHE_STORE_ATOMICS";

    @NotNull
    public static CacheConfiguration<String, Object> getConfig() {
        CacheConfiguration<String, Object> atomics = new CacheConfiguration<>();
        atomics.setName(NAME);
        atomics.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        atomics.setCacheMode(CacheMode.REPLICATED);
        return atomics;
    }

    public static <V> Reference<V> getReference(Ignite ignite, final String name, boolean create) {
        final IgniteCache<Object, Object> cache = create ? ignite.getOrCreateCache(NAME) : ignite.cache(NAME);
        return new Reference<V>() {
            @Override
            public V get() {
                return cache == null ? null : (V) cache.get(name);
            }

            @Override
            public void set(V value) {
                if (value == null) {
                    cache.remove(name);
                } else {
                    cache.put(name, value);
                }
            }
        };
    }
}
