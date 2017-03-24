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

package org.apache.ignite.load.simulation;

import java.io.Serializable;

/**
 * Model of a transaction of money transfer from one account to another.
 */
public class AccountTransaction implements Serializable {
    /** */
    private final AccountTransactionKey transactionKey;

    /** */
    private final int fromAccountId;

    /** */
    private final int toAccountId;

    /** */
    private final double moneyAmount;

    /** */
    public AccountTransaction(AccountTransactionKey transactionKey, int fromAccountId, int toAccountId, double moneyAmount) {
        this.transactionKey = transactionKey;
        this.fromAccountId = fromAccountId;
        this.toAccountId = toAccountId;
        this.moneyAmount = moneyAmount;
    }

    /** */
    public AccountTransactionKey getTransactionId() {
        return transactionKey;
    }

    /** */
    public long getFromAccountId() {
        return fromAccountId;
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    public long getToAccountId() {
        return toAccountId;
    }

    /** */
    public double getMoneyAmount() {
        return moneyAmount;
    }
}
