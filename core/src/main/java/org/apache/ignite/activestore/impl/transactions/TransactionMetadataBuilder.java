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

import java.util.ArrayList;
import java.util.List;

import org.apache.ignite.lang.IgniteBiTuple;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/10/2017 4:22 PM
 */
public class TransactionMetadataBuilder {
    private final List<IgniteBiTuple<String, List>> cachesKeys = new ArrayList<>();

    private long transactionId;

    private List<Object> currentCacheKeys;

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public void addKey(Object key) {
        currentCacheKeys.add(key);
    }

    public void addNextCache(String cacheName, int size) {
        currentCacheKeys = new ArrayList<>(size);
        cachesKeys.add(new IgniteBiTuple<String, List>(cacheName, currentCacheKeys));
    }

    public TransactionMetadata build() {
        return new TransactionMetadata(transactionId, cachesKeys);
    }
}
