/*
 * Copyright (c) 2017. EPAM Systems.
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

package com.epam.lagerta.resources;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EmbeddedKafka implements Resource {
    private static final int ZOOKEEPER_PORT = 2181;
    private static final int BASE_KAFKA_PORT = 9092;
    private static final String LOCALHOST = "localhost";
    private static final long ZOOKEEPER_AWAIT_TIME = 5_000;

    private final EmbeddedZookeeper zookeeperServer = new EmbeddedZookeeper();
    private final List<KafkaServerStartable> brokers = new ArrayList<>();

    private final TemporaryDirectory folder;
    private final int numberOfKafkaBrokers;
    private final int zookeeperPort;
    private final int kafkaPort;

    private Thread zookeeperThread;

    public EmbeddedKafka(TemporaryDirectory folder, int numberOfKafkaBrokers, int zookeeperPort, int kafkaPort) {
        this.folder = folder;
        this.numberOfKafkaBrokers = numberOfKafkaBrokers;
        this.zookeeperPort = zookeeperPort;
        this.kafkaPort = kafkaPort;
    }

    public EmbeddedKafka(TemporaryDirectory folder, int numberOfKafkaBrokers) {
        this(folder, numberOfKafkaBrokers, ZOOKEEPER_PORT, BASE_KAFKA_PORT);
    }

    @Override
    public void setUp() throws Exception {
        startZookeeper();
        startKafkaServers();
    }

    private void startZookeeper() throws IOException, InterruptedException {
        File zkDir = folder.mkSubDir("embedded-zk-" + zookeeperPort);
        ServerConfig config = new ServerConfig();

        config.parse(new String[]{String.valueOf(zookeeperPort), zkDir.getAbsolutePath()});
        zookeeperThread = new Thread(() -> {
            try {
                zookeeperServer.runFromConfig(config);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        zookeeperThread.setDaemon(true);
        zookeeperThread.start();
        // Await zookeeper startup.
        zookeeperThread.join(ZOOKEEPER_AWAIT_TIME);
    }

    private void startKafkaServers() throws IOException {
        for (int i = 0; i < numberOfKafkaBrokers; i++) {
            int port = kafkaPort + i;
            File logDir = folder.mkSubDir(String.format("kafka-local-%s-%s", kafkaPort, i));

            Properties properties = new Properties();
            properties.setProperty(KafkaConfig.ZkConnectProp(), String.format("%s:%s", LOCALHOST, zookeeperPort));
            properties.setProperty(KafkaConfig.BrokerIdProp(), String.valueOf(i + 1));
            properties.setProperty(KafkaConfig.HostNameProp(), LOCALHOST);
            properties.setProperty(KafkaConfig.PortProp(), Integer.toString(port));
            properties.setProperty(KafkaConfig.LogDirProp(), logDir.getAbsolutePath());
            properties.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), String.valueOf(1));
            properties.setProperty(KafkaConfig.LogFlushIntervalMsProp(), String.valueOf(Long.MAX_VALUE));
            properties.setProperty(KafkaConfig.ControlledShutdownEnableProp(), String.valueOf(false));
            properties.setProperty(KafkaConfig.DeleteTopicEnableProp(), String.valueOf(true));
            properties.setProperty(KafkaConfig.NumPartitionsProp(), String.valueOf(numberOfKafkaBrokers));

            KafkaServerStartable broker = startBroker(properties);
            brokers.add(broker);
        }
    }

    private KafkaServerStartable startBroker(Properties props) {
        KafkaServerStartable server = new KafkaServerStartable(new KafkaConfig(props));
        server.startup();
        return server;
    }

    @Override
    public void tearDown() throws InterruptedException {
        for (KafkaServerStartable broker : brokers) {
            broker.shutdown();
            broker.awaitShutdown();
        }
        zookeeperServer.stop();
        zookeeperThread.join(ZOOKEEPER_AWAIT_TIME);
    }

    private static class EmbeddedZookeeper extends ZooKeeperServerMain {
        void stop() {
            super.shutdown();
        }
    }
}
