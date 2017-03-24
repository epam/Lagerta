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

package org.apache.ignite.activestore.impl.publisher;

import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.KeyValueListener;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.lifecycle.LifecycleAware;

import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrei_Yakushin
 * @since 1/23/2017 12:51 PM
 */
public class LastTransactionListener implements KeyValueListener, LifecycleAware {
    private static final Set<Long> transactions = new GridConcurrentHashSet<>();

    @Override
    public void writeTransaction(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates) throws CacheWriterException {
        transactions.add(transactionId);
    }

    @Override
    public void writeGapTransaction(long transactionId) {
        transactions.add(transactionId);
    }

    public Collection<Long> getTransactions() {
        return transactions;
    }

    @Override
    public void start() throws IgniteException {
        //do nothing
    }

    @Override
    public void stop() throws IgniteException {
        transactions.clear();
    }
}
