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

package org.apache.ignite.activestore.rules;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

/**
 * @author Aleksandr_Meterko
 * @since 10/31/2016
 */
public class EmbeddedKafkaRule extends ExternalResource {

    private static final int ZOOKEEPER_PORT = 2181;
    private static final int ZOOKEEPER_TICK_TIME = 500;
    private static final int BASE_KAFKA_PORT = 9092;
    private static final String LOCALHOST = "localhost";

    private ServerCnxnFactory factory;
    private List<KafkaServerStartable> brokers = new ArrayList<>();
    private TemporaryFolder folder = new TemporaryFolder(Paths.get("").toAbsolutePath().toFile());

    private int numberOfKafkaBrokers;

    public EmbeddedKafkaRule(int numberOfKafkaBrokers) {
        this.numberOfKafkaBrokers = numberOfKafkaBrokers;
    }

    @Override
    protected void before() throws IOException {
        folder.create();
        startZookeeper();
        startKafkaServers();
    }

    private void startZookeeper() throws IOException {
        factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress(LOCALHOST, ZOOKEEPER_PORT), 10);
        File snapshotDir = folder.newFolder("embedded-zk-snapshot");
        File logDir = folder.newFolder("embedded-zk-logs");

        try {
            factory.startup(new ZooKeeperServer(snapshotDir, logDir, ZOOKEEPER_TICK_TIME));
        }
        catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private void startKafkaServers() throws IOException {
        for (int i = 0; i < numberOfKafkaBrokers; i++) {
            int port = BASE_KAFKA_PORT + i;
            File logDir = folder.newFolder("kafka-local-" + i);

            Properties properties = new Properties();
            properties.setProperty("zookeeper.connect", String.format("%s:%s", LOCALHOST, ZOOKEEPER_PORT));
            properties.setProperty("broker.id", String.valueOf(i + 1));
            properties.setProperty("host.name", LOCALHOST);
            properties.setProperty("port", Integer.toString(port));
            properties.setProperty("log.dir", logDir.getAbsolutePath());
            properties.setProperty("log.flush.interval.messages", String.valueOf(1));

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
    protected void after() {
        factory.shutdown();
        for (KafkaServerStartable broker : brokers) {
            broker.shutdown();
        }
        folder.delete();
    }

}
