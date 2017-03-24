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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Evgeniy_Ignatiev
 * @since 12:02 11/23/2016
 */
public class TxInfoGenerator {
    private final AtomicInteger txIdCounter = new AtomicInteger();
    private final List<Integer> buckets = new ArrayList<>();
    private final int numberOfBuckets;

    public TxInfoGenerator(int numberOfBuckets) {
        this.numberOfBuckets = numberOfBuckets;
    }

    public TxInfo generateTxInfo() {
        int txId = txIdCounter.getAndIncrement();
        int numberOfBucketsForTx = ThreadLocalRandom.current().nextInt(numberOfBuckets);

        if (buckets.isEmpty()) {
            for (int i = 0; i < numberOfBuckets; i++) {
                buckets.add(i);
            }
        }
        Collections.shuffle(buckets); // Randomize locked buckets per transaction.
        return new TxInfo(txId, buckets.subList(0, numberOfBucketsForTx));
    }

    public List<TxInfo> generateTxInfoBatch(int batchSize) {
        List<TxInfo> result = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            result.add(generateTxInfo());
        }
        return result;
    }

    public List<List<TxInfo>> generateBatchesForPartitions(int numberOfPartitions, int batchSizePerPartition) {
        List<List<TxInfo>> result = new ArrayList<>(numberOfPartitions);

        for (int i = 0; i < numberOfPartitions; i++) {
            List<TxInfo> batch = generateTxInfoBatch(batchSizePerPartition);

            // Make txs out-of-order.
            Collections.shuffle(batch);
            result.add(batch);
        }
        return result;
    }
}
