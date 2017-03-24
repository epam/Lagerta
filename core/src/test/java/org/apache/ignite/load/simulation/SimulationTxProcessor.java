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

import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteRunnable;
import org.apache.ignite.activestore.commons.retry.Retries;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.load.statistics.Statistics;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/11/2017 6:15 PM
 */
class SimulationTxProcessor implements IgniteRunnable {
    private final TransactionData txData;

    @IgniteInstanceResource
    private transient Ignite ignite;

    public SimulationTxProcessor(TransactionData txData) {
        this.txData = txData;
    }

    @Override public void run() {
        IgniteCache accountCache = ignite.cache(SimulationUtil.ACCOUNT_CACHE);
        Retries.tryMe(
            new OptimisticTxProcessor(accountCache, txData),
            Retries.defaultStrategy().onFailure(new RecordingRetryTask())
        );
    }

    private static double queryAccountBalance(
        Account account,
        IgniteCache<AccountTransactionKey, AccountTransaction> accountTxCache
    ) {
        double result = 0;
        int accountId = account.getAccountKey().getAccountId();

        for (AccountTransactionKey txKey : account.getTransactionKeys().values()) {
            AccountTransaction transaction = accountTxCache.get(txKey);

            if (accountId == transaction.getFromAccountId()) {
                result -= transaction.getMoneyAmount();
            }
            else {
                result += transaction.getMoneyAmount();
            }
        }
        return result;
    }

    private static class OptimisticTxProcessor implements Runnable {
        private final IgniteCache accountCache;
        private final TransactionData txData;

        OptimisticTxProcessor(IgniteCache accountCache, TransactionData txData) {
            this.accountCache = accountCache;
            this.txData = txData;
        }

        @SuppressWarnings("unchecked")
        @Override public void run() {
            AccountKey fromAccountKey = new AccountKey(txData.getFromAccountId(), txData.getPartition());
            Account fromAccount = (Account)accountCache.get(fromAccountKey);
            double withdrawBalance = queryAccountBalance(fromAccount, accountCache);

            // ToDo: check made to always succeed - later remove "abs" and substitute + with -.
            if (Math.abs(withdrawBalance) + txData.getMoneyAmount() >= 0) {
                AccountTransactionKey txKey = new AccountTransactionKey(txData.getTransactionId(),
                    txData.getPartition());
                AccountTransaction accountTx = new AccountTransaction(
                    txKey,
                    txData.getFromAccountId(),
                    txData.getToAccountId(),
                    txData.getMoneyAmount()
                );
                AccountKey toAccountKey = new AccountKey(txData.getToAccountId(), txData.getPartition());
                Account toAccount = (Account)accountCache.get(toAccountKey);
                long timestamp = System.currentTimeMillis();

                // Update accounts in caches.
                fromAccount.removeRandomTransaction();
                toAccount.removeRandomTransaction();
                fromAccount.addTransaction(timestamp, txKey);
                toAccount.addTransaction(timestamp, txKey);
                accountCache.put(fromAccount.getAccountKey(), fromAccount);
                accountCache.put(toAccount.getAccountKey(), toAccount);
                accountCache.put(txKey, accountTx);
            }
        }
    }

    private static class RecordingRetryTask extends ActiveStoreIgniteRunnable {
        @Inject
        private transient Statistics stats;

        @Override public void runInjected() {
            stats.recordRetry();
        }
    }
}
