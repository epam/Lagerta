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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Simple model of information about client account in a banking system.
 */
public class Account implements Serializable {
    /** */
    private final AccountKey accountKey;

    /** */
    private final Map<Long, AccountTransactionKey> transactionKeys = new TreeMap<>();

    /** */
    public Account(AccountKey accountKey) {
        this.accountKey = accountKey;
    }

    /** */
    public AccountKey getAccountKey() {
        return accountKey;
    }

    /** */
    public void addTransaction(long timestamp, AccountTransactionKey txKey) {
        transactionKeys.put(timestamp, txKey);
    }

    /** */
    public Map<Long, AccountTransactionKey> getTransactionKeys() {
        return Collections.unmodifiableMap(transactionKeys);
    }

    void removeRandomTransaction() {
        // ToDo: this is a hack to ensure the account objects will not grow in size during simulation test.
        transactionKeys.remove(transactionKeys.keySet().iterator().next());
    }
}
