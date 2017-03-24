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

package org.apache.ignite.activestore.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.commons.Lazy;
import org.apache.ignite.lang.IgniteClosure;

/**
 * "Persistence" storage for in-memory tests.
 */
public class InMemoryStorage {
    /**
     * Delimiter in map names.
     */
    private static final String DELIMITER = ".";

    /**
     * Real storage.
     */
    private static final Lazy<String, Map> persisted = new Lazy<>(new IgniteClosure<String, Map>() {
        @Override public Map apply(String s) {
            return new HashMap();
        }
    });

    /**
     * Returns storage for data.
     *
     * @param cacheName name of Ignite cache.
     * @param metadata snapshot to get storage for.
     * @return storage.
     */
    public static Map getMap(String cacheName, Metadata metadata) {
        return persisted.get(cacheName + DELIMITER + metadata.getTimestamp());
    }

    /**
     * Finds all keys that were changed in cache.
     *
     * @param metadata to check.
     * @return mapping of cache name to changed keys.
     */
    public static Map<String, Set> findChangedKeysByCache(Metadata metadata) {
        Lazy<String, Set> result = new Lazy<>(new IgniteClosure<String, Set>() {
            @Override public Set apply(String s) {
                return new HashSet();
            }
        });
        Iterator<Map.Entry<String, Map>> iterator = persisted.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map> storage = iterator.next();
            String storageKey = storage.getKey();
            if (storageKey.endsWith(String.valueOf(metadata.getTimestamp()))) {
                int index = storageKey.lastIndexOf(DELIMITER);
                String cacheName = storageKey.substring(0, index);
                result.get(cacheName).addAll(storage.getValue().keySet());
            }
        }
        return result.toMap();
    }

}
