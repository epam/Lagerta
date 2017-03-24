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

package org.apache.ignite.activestore.investigation.replication;

import java.util.Collections;
import java.util.List;

/**
 * @author Evgeniy_Ignatiev
 * @since 12:01 11/23/2016
 */
public class TxInfo {
    private final int txId;
    private final List<Integer> bucketIds;

    public TxInfo(int txId, List<Integer> bucketIds) {
        this.txId = txId;
        this.bucketIds = bucketIds;
    }

    public int getTxId() {
        return txId;
    }

    public List<Integer> getBucketIds() {
        return Collections.unmodifiableList(bucketIds);
    }
}
