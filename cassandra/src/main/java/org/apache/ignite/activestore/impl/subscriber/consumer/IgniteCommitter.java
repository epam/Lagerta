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

import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.subscriber.Committer;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.transactions.Transaction;

/**
 * @author Aleksandr_Meterko
 * @since 11/30/2016
 */
public class IgniteCommitter implements Committer {

    private final Ignite ignite;

    public IgniteCommitter(Ignite ignite) {
        this.ignite = ignite;
    }

    @Override public void commit(List<Long> txIds,
        IgniteClosure<Long, Map.Entry<List<IgniteBiTuple<String, ?>>, List<Object>>> txSupplier,
        IgniteInClosure<Long> onSingleCommit) {
        for (final Long txId : txIds) {
            Map.Entry<List<IgniteBiTuple<String, ?>>, List<Object>> currentTx = txSupplier.apply(txId);
            List<IgniteBiTuple<String, ?>> keys = currentTx.getKey();
            List<Object> values = currentTx.getValue();
            try (Transaction tx = ignite.transactions().txStart()) {
                for (int i = 0; i < keys.size(); i++) {
                    IgniteBiTuple<String, ?> compositeKey = keys.get(i);
                    ignite.cache(compositeKey.getKey()).put(compositeKey.getValue(), values.get(i));
                    tx.commit();
                }
            }
            onSingleCommit.apply(txId);
        }
    }

}
