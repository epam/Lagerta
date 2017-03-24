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

import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCondition;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.activestore.load.statistics.Statistics;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;

/**
 * Utility class providing methods facilitating banking transactions processing simulation.
 */
public final class SimulationUtil {
    /** */
    private static final ResourceBundle TESTS_SETTINGS = ResourceBundle.getBundle("simulation-test");

    /** Name of a cache used to store account information. */
    public static final String ACCOUNT_CACHE = "account_cache";

    /** Name of a cache used to store transactions information. */
    public static final String ACCOUNT_TX_CACHE = "tx_cache";

    /** Total number of client accounts in a simulated banking system. */
    public static final int TOTAL_ACCOUNTS = getIntSetting("simulation.test.total.accounts");

    /** A length of a generator sequence chunk per each simulation worker. */
    public static final int WORKER_OFFSET = getIntSetting("simulation.test.worker.offset");

    /** */
    private static final double INITIAL_ACCOUNT_MONEY = getDoubleSetting("simulation.test.initial.account.money");

    /** */
    public static final double MAX_TRANSFERABLE_MONEY = getDoubleSetting("simulation.test.transaction.money.limit");

    /** */
    public static final int SENDER_MAX_ACCOUNT = getIntSetting("simulation.test.sender.account.max");

    /** */
    public static final int RECEIVER_MAX_ACCOUNT = getIntSetting("simulation.test.receiver.account.max");

    /** */
    public static final int INITIAL_ACCOUNT_TXS = getIntSetting("simulation.test.account.initial.transactions");

    /** */
    private static final int PREPOPULATION_BATCH_LIMIT = getIntSetting("simulation.test.prepopulation.batch.limit");

    /** */
    public static final int PARTITIONS_NUMBER = getIntSetting("simulation.test.cache.partitions");

    /** */
    private static final int NONEXISTENT_ACCOUNT_ID = -1;

    /** */
    private static final long TRANSACTION_RETRY_TIMEOUT = 100;

    /** */
    private static final String SIMULATION_CACHES_STATE = "simulationCachesState";

    /** */
    private static final String PREPOPULATED_LOCK = "prepopulatedLock";

    /** */
    private static final String PREPOPULATED_CONDITION = "prepopulatedCondition";

    /** */
    private static final long TRANSACTIONS_PER_BUCKET = 1000 * 1000 * 1000;

    /** */
    private static final String CURRENT_TX_BUCKET_INDEX = "testClientNumber";

    /** */
    private static int getIntSetting(String name) {
        return Integer.parseInt(TESTS_SETTINGS.getString(name));
    }

    /** */
    private static double getDoubleSetting(String name) {
        return Double.parseDouble(TESTS_SETTINGS.getString(name));
    }

    /**
     * Executes simulation transaction on the ignite server in a compute. In a transaction at first checks if the
     * transfer of money is possible, i.e. the drafting account has enough money, then updates both accounts and writes
     * transaction into the cache.
     *
     * In order to avoid deadlock and excessive waiting uses optimistic repeatable-read transactions and retries them in
     * case of optimistic exception.
     *
     * @param ignite
     *     Ignite client.
     */
    public static void processTransaction(Ignite ignite, final TransactionData txData) {
        final AccountKey fromAccountKey = new AccountKey(txData.getFromAccountId(), txData.getPartition());

        ignite.compute().affinityRun(ACCOUNT_CACHE, fromAccountKey, new IgniteRunnable() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public void run() {
                boolean committed = false;

                while (!committed) {
                    try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC,
                        TransactionIsolation.SERIALIZABLE)) {
                        IgniteCache accountCache = ignite.cache(ACCOUNT_CACHE);
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
                        tx.commit();
                        committed = true;
                    }
                    catch (TransactionOptimisticException e) {
                        Statistics.recordRetry();
                        // Retry transaction after timeout.
                        try {
                            Thread.sleep(TRANSACTION_RETRY_TIMEOUT);
                        }
                        catch (InterruptedException ie) {
                            // Do nothing.
                        }
                    }
                }
            }
        });
    }

    /** */
    public static int getPartitionForAccountId(int accountId) {
        return accountId % PARTITIONS_NUMBER;
    }

    /** */
    private static double queryAccountBalance(Account account,
        IgniteCache<AccountTransactionKey, AccountTransaction> accountTxCache) {
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

    /**
     * Populates simulation caches with with a preconfigured number of accounts, each having some amount money to run
     * transfer transactions.
     */
    public static void prepopulateSimulationCaches(Ignite ignite) {
        IgniteLock lock = null;

        try {
            IgniteAtomicReference<SimulationCachesState> state = ignite.atomicReference(
                SIMULATION_CACHES_STATE, SimulationCachesState.UNPOPULATED, true);

            if (!(state.get() == SimulationCachesState.POPULATED)) {
                lock = ignite.reentrantLock(PREPOPULATED_LOCK, true, false, true);

                IgniteCondition condition = lock.getOrCreateCondition(PREPOPULATED_CONDITION);
                boolean shouldPopulate = false;

                lock.lock();
                try {
                    if (state.compareAndSet(SimulationCachesState.UNPOPULATED, SimulationCachesState.POPULATING)) {
                        shouldPopulate = true;
                    }
                    else if (state.get() == SimulationCachesState.POPULATING) {
                        condition.await();
                    }
                }
                finally {
                    lock.unlock();
                }

                if (!shouldPopulate) {
                    return;
                }
                Map<AccountKey, Account> accounts = new HashMap<>();
                Map<AccountTransactionKey, AccountTransaction> accountTxs = new HashMap<>();

                preparePopulateBatch(0, TOTAL_ACCOUNTS, 0, accounts, accountTxs);
                ignite.atomicLong(CURRENT_TX_BUCKET_INDEX, 0, true);
                writeBatchSafe(ignite, ACCOUNT_CACHE, accounts);
                writeBatchSafe(ignite, ACCOUNT_CACHE, accountTxs);

                lock.lock();
                try {
                    state.set(SimulationCachesState.POPULATED);
                    condition.signalAll();
                }
                finally {
                    lock.unlock();
                }
            }
        }
        finally {
            if (lock != null && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    public static void preparePopulateBatch(int startAccountId, int endAccountId, long startTxId,
        Map<AccountKey, Account> accounts, Map<AccountTransactionKey, AccountTransaction> accountTxs) {
        double moneyPerInitialTx = INITIAL_ACCOUNT_MONEY / INITIAL_ACCOUNT_TXS;
        long currentTxId = startTxId;

        for (int i = startAccountId; i < endAccountId; i++) {
            int partition = getPartitionForAccountId(i);
            AccountKey accountKey = new AccountKey(i, partition);
            Account account = new Account(accountKey);

            for (long j = 0; j < INITIAL_ACCOUNT_TXS; j++) {
                AccountTransactionKey transactionKey = new AccountTransactionKey(currentTxId, partition);
                // ToDo: introduce deposit transaction class.
                AccountTransaction accountTx = new AccountTransaction(
                        transactionKey,
                        NONEXISTENT_ACCOUNT_ID,
                        i,
                        moneyPerInitialTx
                );
                long timestamp = System.currentTimeMillis();

                account.addTransaction(timestamp, transactionKey);
                accountTxs.put(transactionKey, accountTx);
                currentTxId++;
            }
            accounts.put(accountKey, account);
        }
    }

    public static long getNextTransactionsIdsBucketStart(Ignite ignite) {
        // First bucket is fully reserved for the prepopulation transactions, so we increment first.
        return TRANSACTIONS_PER_BUCKET * ignite.atomicLong(CURRENT_TX_BUCKET_INDEX, 0, false).incrementAndGet();
    }

    // Created to use in tests to avoid having errors due too writing too large batch in Cassandra.
    private static <K, V> void writeBatchSafe(Ignite ignite, String cacheName, Map<K, V> entries) {
        int batchLimit = PREPOPULATION_BATCH_LIMIT;

        if (entries.size() <= batchLimit) {
            ignite.compute().run(new BatchWriter(cacheName, entries));
            return;
        }
        Map<K, V> batch = new HashMap<>(batchLimit);

        for (Map.Entry<K, V> entry : entries.entrySet()) {
            if (batch.size() == batchLimit) {
                ignite.compute().run(new BatchWriter(cacheName, batch));
                batch.clear();
            }
            batch.put(entry.getKey(), entry.getValue());
        }
        if (!batch.isEmpty()) {
            ignite.compute().run(new BatchWriter(cacheName, batch));
        }
    }

    /** */
    private enum SimulationCachesState {
        UNPOPULATED, POPULATING, POPULATED
    }

    /**
     * Writes a batch of data into the cache.
     */
    private static class BatchWriter implements IgniteRunnable {
        /** */
        private final String cacheName;

        /** */
        private final Map data;
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        BatchWriter(String cacheName, Map data) {
            this.cacheName = cacheName;
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ignite.cache(cacheName).putAll(data);
        }
    }
}
