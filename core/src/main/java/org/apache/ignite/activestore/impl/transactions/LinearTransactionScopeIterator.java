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

import java.util.Iterator;
import java.util.List;

import org.apache.ignite.activestore.transactions.TransactionScopeIterator;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/10/2017 6:33 PM
 */
public class LinearTransactionScopeIterator implements TransactionScopeIterator {
    private final Iterator<IgniteBiTuple<String, List>> cachesKeysIterator;

    private Iterator keyIterator;
    private Object currentKey;

    public LinearTransactionScopeIterator(List<IgniteBiTuple<String, List>> cachesKeys) {
        cachesKeysIterator = cachesKeys.iterator();
    }

    @Override public boolean hasNextEntry() {
        return keyIterator.hasNext();
    }

    @Override public void advance() {
        currentKey = keyIterator.next();
    }

    @Override public Object getKey() {
        return currentKey;
    }

    @Override public boolean hasNextCache() {
        return cachesKeysIterator.hasNext();
    }

    @Override public String nextCache() {
        IgniteBiTuple<String, List> cacheKeys = cachesKeysIterator.next();

        keyIterator = cacheKeys.getValue().iterator();
        return cacheKeys.getKey();
    }
}
