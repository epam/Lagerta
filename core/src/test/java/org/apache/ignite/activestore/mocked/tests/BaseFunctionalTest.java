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

package org.apache.ignite.activestore.mocked.tests;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.activestore.commons.injection.InjectionForTests;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.transactions.TransactionMessage;
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.activestore.mocked.mocks.InputProducer;
import org.apache.ignite.activestore.mocked.mocks.KafkaMockFactory;
import org.apache.ignite.activestore.rules.SingleClusterResource;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/16/2017 6:30 PM
 */
public class BaseFunctionalTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseFunctionalTest.class);

    private static final String CACHE_NAME = "userCache";

    @ClassRule
    public static RuleChain clusterResource = SingleClusterResource.getAllResourcesRule(5 * 60_000);

    private static int currentTxId = 0;

    @Rule
    public RuleChain perTestRules = SingleClusterResource.getPerTestMethodRules();

    protected KafkaMockFactory kafkaMockFactory;
    protected Ignite ignite;
    protected IgniteCache<Object, Object> cache;

    @Before
    public void setUp() {
        ignite = SingleClusterResource.getClusterResource().ignite();
        kafkaMockFactory = (KafkaMockFactory)InjectionForTests.get(KafkaFactory.class, ignite);
        cache = ignite.cache(CACHE_NAME);
    }

    @After
    public void clear() {
        cache.clear();
        InputProducer.resetOffsets();
        KafkaMockFactory.clearState();
        currentTxId = 0;
    }

    protected TransactionMessage createMessage(long txId, List keys, List values) {
        IgniteBiTuple<String, List> cacheKeys = new IgniteBiTuple<>(CACHE_NAME, keys);
        TransactionMetadata metadata = new TransactionMetadata(txId, Collections.singletonList(cacheKeys));
        return new TransactionMessage(metadata, Collections.singletonList(values));
    }

    protected int getNextTxId() {
        return currentTxId++;
    }

    protected void writeValueToRemoteKafka(int key, int value) {
        writeValueToKafka(KafkaMockFactory.REMOTE_ID, key, value);
    }

    protected void writeValueToReconKafka(int key, int value) {
        writeValueToKafka(KafkaMockFactory.RECONCILIATION_ID, key, value);
    }

    protected void writeValueToKafka(String topic, int key, int value) {
        writeValueToKafka(topic, getNextTxId(), key, value);
    }

    protected void writeValueToKafka(String topic, int id, int key, int value) {
        InputProducer producer = kafkaMockFactory.inputProducer(topic, 0);
        producer.send(createMessage(id, Collections.singletonList(key), Collections.singletonList(value)));
    }
}
