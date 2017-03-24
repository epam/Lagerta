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

package org.apache.ignite.load;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteRunnable;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/11/2017 6:49 PM
 */
class EntriesProcessingTask extends ActiveStoreIgniteRunnable {
    public static final String PROCESSING_CLOSURE_BIND_KEY = "processingClosure";

    private final Map entries;

    @Inject
    private transient LoadTestConfig config;

    @Inject
    @Named(PROCESSING_CLOSURE_BIND_KEY)
    private transient IgniteBiInClosure<IgniteCache, Map<?, ?>> processingClosure;

    @IgniteInstanceResource
    private transient Ignite ignite;

    public EntriesProcessingTask(Map entries) {
        this.entries = entries;
    }

    @Override public void runInjected() {
        IgniteCache cache = ignite.cache(config.getCacheName());

        if (config.isTransactional()) {
            try (Transaction tx = ignite.transactions().txStart()) {
                processingClosure.apply(cache, entries);
                tx.commit();
            }
        }
        else {
            processingClosure.apply(cache, entries);
        }
    }
}
