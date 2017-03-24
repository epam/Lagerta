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

package org.apache.ignite.activestore.impl.publisher;

import org.apache.ignite.activestore.KeyValueListener;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import javax.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/8/2016 1:57 PM
 */
public class LocalKafkaKVListener implements KeyValueListener, LifecycleAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalKafkaKVListener.class);

    @Inject
    private LocalKafkaProducer producer;

    @Override public void writeTransaction(long transactionId,
        Map<String, Collection<Cache.Entry<?, ?>>> updates) throws CacheWriterException {
        producer.writeTransaction(transactionId, updates);
    }

    @Override public void writeGapTransaction(long transactionId) {
        writeTransaction(transactionId, Collections.<String, Collection<Cache.Entry<?, ?>>>emptyMap());
    }

    @Override public void start() {
        // Do nothing.
    }

    @Override public void stop() {
        try {
            producer.close();
        } catch (Exception e) {
            LOGGER.error("[G] Exception while closing producer: ", e);
        }
    }
}
