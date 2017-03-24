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

package org.apache.ignite.activestore.commons;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.lang.IgniteClosure;

/**
 * Substitute for lazy evaluated map. Allows to always retrieve non-null values from inner map by calling factory to
 * build object for null values.
 */
public class Lazy<K, V> implements Iterable<Map.Entry<K, V>> {
    /**
     * Inner storage of data.
     */
    private final Map<K, V> map;

    /**
     * Factory for creating new objects.
     */
    private final IgniteClosure<K, V> factory;

    /**
     * Creates instance with given settings.
     *
     * @param factory for creating new objects.
     */
    public Lazy(IgniteClosure<K, V> factory) {
        this(new HashMap<K, V>(), factory);
    }

    /**
     * Creates instance with given settings.
     *
     * @param map with initial values.
     * @param factory for creating new objects.
     */
    public Lazy(Map<K, V> map, IgniteClosure<K, V> factory) {
        this.map = map;
        this.factory = factory;
    }

    /**
     * Gets value from map.
     *
     * @param key to get value.
     * @return value.
     */
    public V get(K key) {
        V v;
        if ((v = map.get(key)) == null) {
            V newValue;
            if ((newValue = factory.apply(key)) != null) {
                map.put(key, newValue);
                return newValue;
            }
        }

        return v;
    }

    /**
     * Removes value from map.
     *
     * @param key to remove value.
     */
    public void remove(K key) {
        map.remove(key);
    }

    /**
     * Returns iterator over inner storage.
     *
     * @return iterator.
     */
    @Override public Iterator<Map.Entry<K, V>> iterator() {
        return map.entrySet().iterator();
    }

    /**
     * Returns inner storage.
     *
     * @return map.
     */
    public Map<K, V> toMap() {
        return new HashMap<>(map);
    }
}
