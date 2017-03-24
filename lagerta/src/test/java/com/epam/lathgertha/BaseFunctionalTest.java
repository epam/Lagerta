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
import com.epam.lathgertha.rules.IgniteClusterResource;
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
    private static final String CACHE_NAME = "someCache";
    private static final String CONFIG_PATH = "/com/epam/lathgertha/functional/config.xml";
    private static final ConfigurableApplicationContext CONTEXT = new ClassPathXmlApplicationContext(CONFIG_PATH);
    private static IgniteClusterResource clusterResource = new IgniteClusterResource(1, new AppContextOneProcessClusterManager(CONTEXT));

    protected KafkaMockFactory kafkaMockFactory;
    protected Ignite ignite;

    private int currentTxId;

    @BeforeSuite
    public void initCluster() {
        ignite = clusterResource.startCluster();
    }

    @AfterSuite
    public void stopCluster() {
        clusterResource.stopCluster();
    }

    @BeforeMethod
    public void clearCluster() {
        clusterResource.clearCluster();
        kafkaMockFactory = CONTEXT.getBean(KafkaMockFactory.class);
    }

    @AfterMethod
    public void clearState() {
        CONTEXT.refresh();
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
