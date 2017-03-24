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

import org.apache.ignite.activestore.commons.serializer.Serializer;
import org.apache.ignite.activestore.impl.transactions.LinearTransactionDataIterator;
import org.apache.ignite.activestore.subscriber.TransactionSupplier;
import org.apache.ignite.activestore.transactions.TransactionDataIterator;
import org.apache.ignite.activestore.transactions.TransactionScopeIterator;

import javax.inject.Inject;
import java.util.List;

/**
 * @author Andrei_Yakushin
 * @since 12/19/2016 9:07 AM
 */
class DeserializerClosure extends TransactionsBufferHolder<DeserializerClosure> implements TransactionSupplier {
    private final Serializer serializer;

    @Inject
    public DeserializerClosure(Serializer serializer) {
        this.serializer = serializer;
    }

    @Override public TransactionScopeIterator scopeIterator(long txId) {
        TransactionWrapper currentTx = buffer.getInProgressTx(txId);
        return currentTx.deserializedMetadata().scopeIterator();
    }

    @SuppressWarnings("unchecked")
    @Override public TransactionDataIterator dataIterator(long txId) {
        TransactionWrapper currentTx = buffer.getInProgressTx(txId);
        List<List> values = serializer.deserialize(currentTx.data());
        return new LinearTransactionDataIterator(currentTx.deserializedMetadata().scopeIterator(), values);
    }
}
