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

import com.epam.lathgertha.cluster.AppContextOneProcessClusterManager;
import com.epam.lathgertha.mocks.InputProducer;
import com.epam.lathgertha.mocks.KafkaMockFactory;
import com.epam.lathgertha.resources.IgniteClusterResource;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

import javax.cache.Cache;
import java.util.Collection;
import java.util.Collections;

public abstract class BaseFunctionalTest {
    // configured topic name in config.xml
    protected final String TOPIC = "testTopic";
    protected final String CACHE_NAME = "someCache";
    private static final String CONFIG_PATH = "/com/epam/lathgertha/functional/config.xml";
    private static final ConfigurableApplicationContext CONTEXT = new ClassPathXmlApplicationContext(CONFIG_PATH);
    private static final AppContextOneProcessClusterManager CLUSTER_MANAGER =
            new AppContextOneProcessClusterManager(CONFIG_PATH);
    private static final IgniteClusterResource CLUSTER_RESOURCE =
            new IgniteClusterResource(2, CLUSTER_MANAGER);

    protected KafkaMockFactory kafkaMockFactory;
    protected Ignite ignite;

    private int currentTxId;

    @BeforeSuite
    public void initCluster() {
        CLUSTER_RESOURCE.setUp();
        ignite = CLUSTER_RESOURCE.ignite();
    }

    @AfterSuite
    public void stopCluster() {
        CLUSTER_RESOURCE.tearDown();
    }

    @BeforeMethod
    public void clearCluster() {
        CLUSTER_RESOURCE.clearCluster();
        kafkaMockFactory = CONTEXT.getBean(KafkaMockFactory.class);
    }

    @AfterMethod
    public void clearState() {
        CLUSTER_MANAGER.refreshContexts();
        InputProducer.resetOffsets();
        KafkaMockFactory.clearState();
    }

    protected int getNextTxId() {
        return currentTxId++;
    }

    protected void writeValueToKafka(String topic, int key, int value) {
        writeValueToKafka(topic, getNextTxId(), key, value);
    }

    protected void writeValueToKafka(String topic, int id, int key, int value) {
        InputProducer producer = kafkaMockFactory.inputProducer(topic, 0);
        Collection<Cache.Entry<?, ?>> updates = Collections.singletonList(new CacheEntryImpl<>(key, value));
        producer.send(id, Collections.singletonMap(CACHE_NAME, updates));
    }
}
