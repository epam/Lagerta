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

import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteCondition;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.load.PropertiesParser;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Utility class providing methods facilitating banking transactions processing simulation.
 */
public final class SimulationUtil {
    private static final PropertiesParser SETTINGS = new PropertiesParser("/load/simulation-test.properties");

    /** Name of a cache used to store account information. */
    public static final String ACCOUNT_CACHE = "account_cache";

    /** Name of a cache used to store transactions information. */
    public static final String ACCOUNT_TX_CACHE = "tx_cache";

    /** Total number of client accounts in a simulated banking system. */
    public static final int TOTAL_ACCOUNTS = SETTINGS.parseIntSetting("simulation.test.total.accounts");

    /** A length of a generator sequence chunk per each simulation worker. */
    public static final int WORKER_OFFSET = SETTINGS.parseIntSetting("simulation.test.worker.offset");

    /** */
    private static final double INITIAL_ACCOUNT_MONEY = SETTINGS.parseDoubleSetting(
        "simulation.test.initial.account.money");

    /** */
    public static final double MAX_TRANSFERABLE_MONEY = SETTINGS.parseDoubleSetting(
        "simulation.test.transaction.money.limit");

    /** */
    public static final int SENDER_MAX_ACCOUNT = SETTINGS.parseIntSetting("simulation.test.sender.account.max");

    /** */
    public static final int RECEIVER_MAX_ACCOUNT = SETTINGS.parseIntSetting("simulation.test.receiver.account.max");

    /** */
    public static final int INITIAL_ACCOUNT_TXS = SETTINGS.parseIntSetting(
        "simulation.test.account.initial.transactions");

    /** */
    private static final int PREPOPULATION_BATCH_LIMIT = SETTINGS.parseIntSetting(
        "simulation.test.prepopulation.batch.limit");

    /** */
    public static final int PARTITIONS_NUMBER = SETTINGS.parseIntSetting("simulation.test.cache.partitions");

    /** */
    private static final int NONEXISTENT_ACCOUNT_ID = -1;

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
    public static void processTransaction(Ignite ignite, TransactionData txData) {
        AccountKey fromAccountKey = new AccountKey(txData.getFromAccountId(), txData.getPartition());
        ignite.compute().affinityRun(ACCOUNT_CACHE, fromAccountKey, new SimulationTxProcessor(txData));
    }

    /** */
    public static int getPartitionForAccountId(int accountId) {
        return accountId % PARTITIONS_NUMBER;
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

    // Created to use in tests to avoid having errors due to writing too large batch in underlying bus.
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

    private enum SimulationCachesState {
        UNPOPULATED, POPULATING, POPULATED
    }

    /**
     * Writes a batch of data into the cache.
     */
    private static class BatchWriter implements IgniteRunnable {
        private final String cacheName;

        private final Map data;

        @IgniteInstanceResource
        private transient Ignite ignite;

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
