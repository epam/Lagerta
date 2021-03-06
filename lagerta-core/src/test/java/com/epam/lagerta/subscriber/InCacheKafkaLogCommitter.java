/*
 * Copyright (c) 2017. EPAM Systems.
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

package com.epam.lagerta.subscriber;

import com.epam.lagerta.kafka.KafkaLogCommitter;
import org.apache.ignite.Ignite;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class InCacheKafkaLogCommitter implements KafkaLogCommitter {

    public static final String COMMITTED_TRANSACTIONS_COUNT_CACHE_NAME = "committedTransactionsCountCache";

    private final Ignite ignite;

    public InCacheKafkaLogCommitter(Ignite ignite) {
        this.ignite = ignite;
    }

    @Override
    public Future<RecordMetadata> commitTransaction(long transactionId) {
        ignite.cache(COMMITTED_TRANSACTIONS_COUNT_CACHE_NAME).put(transactionId, 0);
        return null;
    }

    @Override
    public void close() {
        //do nothing
    }
}
