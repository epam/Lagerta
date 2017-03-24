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
import java.util.Collection;
import java.util.List;

import javax.cache.Cache;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/10/2017 4:22 PM
 */
public class TransactionMessageBuilder {
    private final TransactionMetadataBuilder metadataBuilder = new TransactionMetadataBuilder();
    private final List<List> values = new ArrayList<>();

    public void setTransactionId(long transactionId) {
        metadataBuilder.setTransactionId(transactionId);
    }

    public void addCacheEntries(String cacheName, Collection<Cache.Entry<?, ?>> entries) {
        List<Object> cacheValues = new ArrayList<>(entries.size());

        metadataBuilder.addNextCache(cacheName, entries.size());
        values.add(cacheValues);
        for (Cache.Entry<?, ?> entry : entries) {
            metadataBuilder.addKey(entry.getKey());
            cacheValues.add(entry.getValue());
        }
    }

    public TransactionMessage build() {
        return new TransactionMessage(metadataBuilder.build(), values);
    }
}
