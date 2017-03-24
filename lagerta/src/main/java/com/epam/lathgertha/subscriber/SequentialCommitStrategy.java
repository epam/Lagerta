/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.kafka.KafkaLogCommitter;
import com.epam.lathgertha.util.Serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SequentialCommitStrategy implements CommitStrategy {

    private final Committer committer;
    private final Serializer serializer;
    private final KafkaLogCommitter kafkaLogCommitter;

    public SequentialCommitStrategy(Serializer serializer, Committer committer, KafkaLogCommitter kafkaLogCommitter) {
        this.serializer = serializer;
        this.committer = committer;
        this.kafkaLogCommitter = kafkaLogCommitter;
    }

    @Override
    public void commit(List<Long> txIdsToCommit, Map<Long, Map.Entry<TransactionScope, ByteBuffer>> transactionsBuffer) {

        for (Long txId : txIdsToCommit) {

            List<String> cacheNames = new ArrayList<>();
            List<List<?>>  keys = new ArrayList<>();

            Map.Entry<TransactionScope, ByteBuffer> transactionScopeAndSerializedValues = transactionsBuffer.get(txId);

            TransactionScope transactionScope = transactionScopeAndSerializedValues.getKey();
            for (Map.Entry<String, List> cacheNameAndKeysList : transactionScope.getScope()) {
                cacheNames.add(cacheNameAndKeysList.getKey());
                keys.add(cacheNameAndKeysList.getValue());
            }

            List<List<?>> values = serializer.<List>deserialize(transactionScopeAndSerializedValues.getValue());

            committer.commit(cacheNames, keys, values);
            kafkaLogCommitter.commitTransaction(txId);
        }
    }
}
