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

import org.apache.ignite.activestore.transactions.TransactionDataIterator;
import org.apache.ignite.activestore.transactions.TransactionScopeIterator;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/10/2017 6:40 PM
 */
public class LinearTransactionDataIterator implements TransactionDataIterator {
    private final TransactionScopeIterator scopeIterator;
    private final Iterator<List> valuesIterator;

    private Iterator currentCacheValuesIterator;
    private Object currentValue;

    public LinearTransactionDataIterator(TransactionScopeIterator scopeIterator, List<List> values) {
        this.scopeIterator = scopeIterator;
        valuesIterator = values.iterator();
    }

    @Override public boolean hasNextEntry() {
        return scopeIterator.hasNextEntry();
    }

    @Override public void advance() {
        currentValue = currentCacheValuesIterator.next();
        scopeIterator.advance();
    }

    @Override public Object getKey() {
        return scopeIterator.getKey();
    }

    @Override public Object getValue() {
        return currentValue;
    }

    @Override public boolean hasNextCache() {
        return valuesIterator.hasNext();
    }

    @Override public String nextCache() {
        currentCacheValuesIterator = valuesIterator.next().iterator();
        return scopeIterator.nextCache();
    }
}
