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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SequentialCommitStrategy implements CommitStrategy {
    private final CommitServitor commitServitor;

    public SequentialCommitStrategy(CommitServitor commitServitor) {
        this.commitServitor = commitServitor;
    }

    @Override
    public List<Long> commit(List<Long> txIdsToCommit, Map<Long, TransactionData> transactionsBuffer) {
        for (Long txId : txIdsToCommit) {
            if (!commitServitor.commit(txId, transactionsBuffer)) {
                return txIdsToCommit.stream().filter(id -> id < txId).collect(Collectors.toList());
            }
        }
        return txIdsToCommit;
    }
}
