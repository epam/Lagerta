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

package org.apache.ignite.activestore.load;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.activestore.load.statistics.Statistics;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;

/**
 * An implementation of {@link WorkerEntryProcessor} that performs all processing on the ignite server node using
 * compute API.
 */
public abstract class ServerSideEntryProcessor implements WorkerEntryProcessor {
    /** Name of the cache to process. */
    private final String cacheName;

    /** Determines if the processing will be done in a transaction or not. */
    private final boolean transactional;

    /** Ignite client used to connect to cluster and send compute. */
    private final Ignite ignite;

    /** */
    public ServerSideEntryProcessor(Ignite ignite, LoadTestConfig config) {
        this.ignite = ignite;
        cacheName = config.getCacheName();
        transactional = config.isTransactional();
    }

    /** {@inheritDoc} */
    @Override public void process(final Map<?, ?> entries) {
        long processingStartTime = System.currentTimeMillis();
        ignite.compute().run(new IgniteRunnable() {
            @IgniteInstanceResource
            private Ignite localIgnite;

            @Override public void run() {
                IgniteCache cache = localIgnite.cache(cacheName);

                if (transactional) {
                    try (Transaction tx = localIgnite.transactions().txStart()) {
                        process(cache, entries);
                    }
                }
                else {
                    process(cache, entries);
                }
            }
        });
        Statistics.recordOperation(System.currentTimeMillis() - processingStartTime);
    }

    /** */
    protected abstract void process(IgniteCache cache, Map<?, ?> entries);
}
