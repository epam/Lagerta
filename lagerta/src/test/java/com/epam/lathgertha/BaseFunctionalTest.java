/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lathgertha;

import com.epam.lathgertha.base.jdbc.H2DataSource;
import com.epam.lathgertha.cluster.AppContextOneProcessClusterManager;
import com.epam.lathgertha.mocks.InputProducer;
import com.epam.lathgertha.mocks.KafkaMockFactory;
import com.epam.lathgertha.resources.IgniteClusterResource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

import javax.cache.Cache;
import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;

public abstract class BaseFunctionalTest {
    // configured topic name in config.xml
    protected final String TOPIC = "testTopic";
    protected final String CACHE_NAME = "someCache";
    protected static final AppContextOneProcessClusterManager CLUSTER_MANAGER =
            new AppContextOneProcessClusterManager("/com/epam/lathgertha/functional/config.xml");
    private static final int NUMBER_OF_NODES = 2;
    private static final IgniteClusterResource CLUSTER_RESOURCE =
            new IgniteClusterResource(NUMBER_OF_NODES, CLUSTER_MANAGER);

    protected KafkaMockFactory kafkaMockFactory;
    protected Ignite ignite;

    private int currentTxId;

    @BeforeSuite
    public void initCluster() {
        CLUSTER_RESOURCE.setUp();
    }

    @AfterSuite
    public void stopCluster() {
        CLUSTER_RESOURCE.tearDown();
    }

    @BeforeClass
    public void getIgnite() {
        ignite = CLUSTER_RESOURCE.ignite();
    }

    @BeforeMethod
    public void setUp() {
        kafkaMockFactory = CLUSTER_MANAGER.getBean(KafkaMockFactory.class);
        kafkaMockFactory.setNumberOfNodes(NUMBER_OF_NODES);
    }

    @AfterMethod
    public void clearState() {
        InputProducer.resetOffsets();
        KafkaMockFactory.clearState();
        CLUSTER_RESOURCE.clearCluster();
    }

    protected BasicDataSource getJdbcDataSource(String url) {
        return H2DataSource.create(url);
    }

    protected int getNextTxId() {
        return currentTxId++;
    }

    protected void writeValueToKafka(String topic, int key, int value) {
        writeValueToKafka(topic, getNextTxId(), key, value);
    }

    protected void writeValueToKafka(String topic, int id, int key, int value) {
        writeValueToKafka(topic, id, key, value, 0);
    }

    protected void writeValueToKafka(String topic, int id, int key, int value, int kafkaPartition) {
        InputProducer producer = kafkaMockFactory.inputProducer(topic, kafkaPartition);
        Collection<Cache.Entry<?, ?>> updates = Collections.singletonList(new CacheEntryImpl<>(key, value));
        producer.send(id, Collections.singletonMap(CACHE_NAME, updates));
    }
}
