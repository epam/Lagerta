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

package org.apache.ignite.activestore.impl.transactions;

import java.util.HashSet;
import java.util.Set;

import org.apache.ignite.activestore.commons.Lazy;
import org.apache.ignite.activestore.transactions.TransactionScopeIterator;
import org.apache.ignite.lang.IgniteClosure;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/10/2017 5:03 PM
 */
public class JointTxScope {
    private static final IgniteClosure<String, Set<Object>> HASH_SET = new IgniteClosure<String, Set<Object>>() {
        @Override public Set<Object> apply(String s) {
            return new HashSet<>();
        }
    };

    private final Lazy<String, Set<Object>> keysPerCache = new Lazy<>(HASH_SET);

    /** Returns true iff the other scope was disjoint with this one. */
    public boolean addAll(TransactionScopeIterator scopeIterator) {
        boolean disjoint = true;

        while (scopeIterator.hasNextCache()) {
            String cacheName = scopeIterator.nextCache();
            Set<Object> keys = keysPerCache.get(cacheName);

            while (scopeIterator.hasNextEntry()) {
                scopeIterator.advance();
                if (!keys.add(scopeIterator.getKey()) && disjoint) {
                    disjoint = false;
                }
            }
        }
        return disjoint;
    }

    public boolean intersects(TransactionScopeIterator scopeIterator) {
        while (scopeIterator.hasNextCache()) {
            String cacheName = scopeIterator.nextCache();
            Set<Object> keys = keysPerCache.get(cacheName);

            if (keys.isEmpty()) {
                continue;
            }
            while (scopeIterator.hasNextEntry()) {
                scopeIterator.advance();
                if (keys.contains(scopeIterator.getKey())) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean containsAll(TransactionScopeIterator scopeIterator) {
        while (scopeIterator.hasNextCache()) {
            String cacheName = scopeIterator.nextCache();
            Set<Object> keys = keysPerCache.get(cacheName);

            while (scopeIterator.hasNextEntry()) {
                scopeIterator.advance();
                if (!keys.contains(scopeIterator.getKey())) {
                    return false;
                }
            }
        }
        return true;
    }
}
