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

package org.apache.ignite.activestore.impl.kv;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * @author Aleksandr_Meterko
 * @since 11/24/2016
 */
public class MessageMetadata implements Serializable {

    private long transactionId;
    //TODO consider removing redundant rows for cacheName
    private List<IgniteBiTuple<String, ?>> compositeKeys = new ArrayList<>();

    public MessageMetadata(long transactionId) {
        this.transactionId = transactionId;
    }

    public void addRow(String cacheName, Object key) {
        compositeKeys.add(new IgniteBiTuple<>(cacheName, key));
    }

    public long getTransactionId() {
        return transactionId;
    }

    public Set<String> getCaches() {
        Set<String> result = new HashSet<>();
        for (IgniteBiTuple<String, ?> compositeKey : compositeKeys) {
            result.add(compositeKey.getKey());
        }
        return result;
    }

    public List<?> getKeysForCache(String cacheName) {
        GridArgumentCheck.notNull(cacheName, "cache name cannot be null");
        List<Object> result = new ArrayList<>();
        for (IgniteBiTuple<String, ?> compositeKey : compositeKeys) {
            if (cacheName.equals(compositeKey.getKey())) {
                result.add(compositeKey.getValue());
            }
        }
        return result;
    }

    public List<IgniteBiTuple<String, ?>> getCompositeKeys() {
        return compositeKeys;
    }
}
