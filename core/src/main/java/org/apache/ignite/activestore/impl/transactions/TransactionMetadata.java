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

import org.apache.ignite.activestore.transactions.TransactionScopeIterator;
import org.apache.ignite.lang.IgniteBiTuple;

import java.io.Serializable;
import java.util.List;

/**
 * @author Aleksandr_Meterko
 * @since 11/24/2016
 */
public class TransactionMetadata implements Serializable {
    private final long transactionId;
    private final List<IgniteBiTuple<String, List>> cachesKeys;

    public TransactionMetadata(long transactionId, List<IgniteBiTuple<String, List>> cachesKeys) {
        this.transactionId = transactionId;
        this.cachesKeys = cachesKeys;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public TransactionScopeIterator scopeIterator() {
        return new LinearTransactionScopeIterator(cachesKeys);
    }

    @Override
    public String toString() {
        return "Tx{" + transactionId + '}';
    }
}
