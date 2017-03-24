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

package org.apache.ignite.activestore.impl.subscriber.consumer;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.activestore.ActiveCacheStore;
import org.apache.ignite.activestore.subscriber.TransactionSupplier;
import org.apache.ignite.activestore.transactions.TransactionDataIterator;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.transactions.Transaction;

/**
 * @author Andrei_Yakushin
 * @since 1/9/2017 11:07 AM
 */
class AbstractIgniteCommitter {
    protected final Ignite ignite;

    AbstractIgniteCommitter(Ignite ignite) {
        this.ignite = ignite;
    }

    protected void singleCommit(TransactionSupplier txSupplier, IgniteInClosure<Long> onSingleCommit, long txId) {
        TransactionDataIterator it = txSupplier.dataIterator(txId);

        try (Transaction tx = ignite.transactions().txStart()) {
            while (it.hasNextCache()) {
                IgniteCache<Object, Object> cache = ignite.cache(it.nextCache());

                while (it.hasNextEntry()) {
                    it.advance();
                    Object value = it.getValue();
                    if (ActiveCacheStore.TOMBSTONE.equals(value)) {
                        cache.remove(it.getKey());
                    } else {
                        cache.put(it.getKey(), value);
                    }
                }
            }
            tx.commit();
        }
        onSingleCommit.apply(txId);
    }
}
