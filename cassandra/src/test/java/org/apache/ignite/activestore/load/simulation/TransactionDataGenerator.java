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

package org.apache.ignite.activestore.load.simulation;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.activestore.load.Generator;

/**
 * Implementation of {@link Generator} that generates money transfer transactions data.
 */
public class TransactionDataGenerator implements Generator {
    /** */
    private static final long RECEIVER_OFFSET = 15172193;

    /** */
    private static final AtomicLong TRANSACTION_ID_COUNTER = new AtomicLong();

    /** {@inheritDoc} */
    @Override public Object generate(long i) {
        int fromAccountId = (int)i % SimulationUtil.SENDER_MAX_ACCOUNT;
        int fromPartition = SimulationUtil.getPartitionForAccountId(fromAccountId);
        int toAccountId = (int)(i + RECEIVER_OFFSET) % SimulationUtil.RECEIVER_MAX_ACCOUNT;
        int toPartition = SimulationUtil.getPartitionForAccountId(toAccountId);
        double moneyAmount = i % SimulationUtil.MAX_TRANSFERABLE_MONEY;
        long transactionId = TRANSACTION_ID_COUNTER.getAndIncrement();
        int resultingPartition = fromPartition;

        if (moneyAmount == 0) {
            moneyAmount = 1;
        }
        if (fromPartition != toPartition) {
            // Avoid making account ids larger or negative so they can not go out of total accounts range.
            if (fromPartition < toPartition) {
                toAccountId -= (toPartition - fromPartition);
            }
            else {
                fromAccountId -= (fromPartition - toPartition);
                resultingPartition = toPartition;
            }
        }
        if (fromAccountId == toAccountId) {
            if (fromAccountId + SimulationUtil.PARTITIONS_NUMBER < SimulationUtil.TOTAL_ACCOUNTS) {
                fromAccountId += SimulationUtil.PARTITIONS_NUMBER;
            }
            else if (fromAccountId - SimulationUtil.PARTITIONS_NUMBER > 0) {
                fromAccountId -= SimulationUtil.PARTITIONS_NUMBER;
            }
            else {
                fromAccountId++; // Only single case with no data locality.
            }
        }
        return new TransactionData(transactionId, fromAccountId, toAccountId, moneyAmount, resultingPartition);
    }

    static void setTransactionIdCounterStart(long start) {
        TRANSACTION_ID_COUNTER.set(start);
    }
}
