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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.rules.TemporaryFolder;

/**
 * @author Aleksandr_Meterko
 * @since 10/31/2016
 */
public class EmbeddedKafkaRule extends MeteredResource {
    private static final String DEFAULT_RESOURCE_NAME = "defaultKafkaResource";
    private static final int ZOOKEEPER_PORT = 2181;
    private static final int ZOOKEEPER_TICK_TIME = 500;
    private static final int BASE_KAFKA_PORT = 9092;
    private static final String LOCALHOST = "localhost";
    private static final int ZOOKEEPER_SESSION_TIMEOUT = 8_000;
    private static final int ZOOKEEPER_CONNECTION_TIMEOUT = 8_000;
    private static final Set<String> INTERNAL_TOPICS = new HashSet<String>() {{
        add("__consumer_offsets");
    }};

    private ServerCnxnFactory factory;
    private final List<KafkaServerStartable> brokers = new ArrayList<>();

    private final TemporaryFolder folder;
    private final int numberOfKafkaBrokers;
    private final int zookeeperPort;
    private final int kafkaPort;

    public EmbeddedKafkaRule(TemporaryFolder folder, String resourceName, int numberOfKafkaBrokers, int zookeeperPort,
        int kafkaPort) {
        super(resourceName);
        this.folder = folder;
        this.numberOfKafkaBrokers = numberOfKafkaBrokers;
        this.zookeeperPort = zookeeperPort;
        this.kafkaPort = kafkaPort;
    }

    public EmbeddedKafkaRule(TemporaryFolder folder, int numberOfKafkaBrokers) {
        this(folder, DEFAULT_RESOURCE_NAME, numberOfKafkaBrokers, ZOOKEEPER_PORT, BASE_KAFKA_PORT);
    }

    @Override
    protected void setUp() {
        try {
            startZookeeper();
            startKafkaServers();
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void startZookeeper() throws IOException, InterruptedException {
        factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress(LOCALHOST, zookeeperPort), 10);
        File snapshotDir = folder.newFolder("embedded-zk-snapshot-" + zookeeperPort);
        File logDir = folder.newFolder("embedded-zk-logs-" + zookeeperPort);
        factory.startup(new ZooKeeperServer(snapshotDir, logDir, ZOOKEEPER_TICK_TIME));
    }

    private void startKafkaServers() throws IOException {
        for (int i = 0; i < numberOfKafkaBrokers; i++) {
            int port = kafkaPort + i;
            File logDir = folder.newFolder(String.format("kafka-local-%s-%s", kafkaPort, i));

            Properties properties = new Properties();
            properties.setProperty("zookeeper.connect", String.format("%s:%s", LOCALHOST, zookeeperPort));
            properties.setProperty("broker.id", String.valueOf(i + 1));
            properties.setProperty("host.name", LOCALHOST);
            properties.setProperty("port", Integer.toString(port));
            properties.setProperty("log.dir", logDir.getAbsolutePath());
            properties.setProperty("log.flush.interval.messages", String.valueOf(1));
            properties.setProperty("log.retention.ms", String.valueOf(Long.MAX_VALUE));
            properties.setProperty("controlled.shutdown.enable", String.valueOf(false));
            properties.setProperty("delete.topic.enable", String.valueOf(true));
            properties.setProperty("num.partitions", String.valueOf(numberOfKafkaBrokers));

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
    protected void tearDown() {
        deleteAllTopics();
        for (KafkaServerStartable broker : brokers) {
            broker.shutdown();
            broker.awaitShutdown();
        }
        factory.shutdown();
    }

    private void deleteAllTopics() {
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;

        try {
            String zookeeperConnect = String.format("%s:%s", LOCALHOST, zookeeperPort);

            zkClient = new ZkClient(
                zookeeperConnect,
                ZOOKEEPER_SESSION_TIMEOUT,
                ZOOKEEPER_CONNECTION_TIMEOUT,
                ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), false);
            for (String topic : listTopics()) {
                if (!INTERNAL_TOPICS.contains(topic)) {
                    AdminUtils.deleteTopic(zkUtils, topic);
                }
            }
        }
        finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    private Collection<String> listTopics() {
        Properties consumerConfig = new Properties() {{
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%s", LOCALHOST, kafkaPort));
            setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
            setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        }};
        try (Consumer<Long, Long> consumer = new KafkaConsumer<>(consumerConfig)) {
            return consumer.listTopics().keySet();
        }
    }
}
