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

/**
 * A kind of DTO for generating and pass information about simulated transfer transaction.
 */
public class TransactionData {
    /** */
    private final long transactionId;

    /** Id of an account from which money are intended to be drafted. */
    private final int fromAccountId;

    /** Id of an account to which money are intended be transferred. */
    private final int toAccountId;

    /** Amount of money to be transferred. */
    private final double moneyAmount;

    /** */
    private final int partition;

    /** */
    public TransactionData(long transactionId, int fromAccountId, int toAccountId, double moneyAmount, int partition) {
        this.transactionId = transactionId;
        this.fromAccountId = fromAccountId;
        this.toAccountId = toAccountId;
        this.moneyAmount = moneyAmount;
        this.partition = partition;
    }

    /** */
    public long getTransactionId() {
        return transactionId;
    }

    /** */
    public int getFromAccountId() {
        return fromAccountId;
    }

    /** */
    public int getToAccountId() {
        return toAccountId;
    }

    /** */
    public double getMoneyAmount() {
        return moneyAmount;
    }

    /** */
    public int getPartition() {
        return partition;
    }
}
