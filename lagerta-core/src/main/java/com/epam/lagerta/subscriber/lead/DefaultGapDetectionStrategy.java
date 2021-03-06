/*
 * Copyright 2017 EPAM Systems.
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

package com.epam.lagerta.subscriber.lead;

import java.util.Collections;
import java.util.List;

public class DefaultGapDetectionStrategy implements GapDetectionStrategy {

    private long lastCheckedDenseCommitted = CommittedTransactions.INITIAL_COMMIT_ID;

    @Override
    public List<Long> gapDetected(CommittedTransactions commited, ReadTransactions read) {
        if (lastCheckedDenseCommitted == CommittedTransactions.INITIAL_COMMIT_ID) {
            lastCheckedDenseCommitted = commited.getLastDenseCommit();
            return Collections.emptyList();
        }
        boolean gapMayExist = commited.isReady() && read.isProbableGap(lastCheckedDenseCommitted);
        boolean noProgressMade = lastCheckedDenseCommitted == commited.getLastDenseCommit();

        return (gapMayExist && noProgressMade)? read.gapsInSparseTransactions() : Collections.emptyList();
    }

}
