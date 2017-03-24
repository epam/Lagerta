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

package org.apache.ignite.activestore.impl.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import javax.inject.Singleton;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * @author Evgeniy_Ignatiev
 * @since 12:19 12/09/2016
 */
@Singleton
public class ClusterGroupService {

    public ClusterGroup getLeadContextLoadingClusterGroup(Ignite ignite, KafkaFactory kafkaFactory,
        DataRecoveryConfig dataRecoveryConfig) {
        String topic = dataRecoveryConfig.getLocalTopic();
        Properties kafkaProperties = dataRecoveryConfig.getConsumerConfig();
        return getClusterGroupAsSubcollection(ignite, getNumberOfPartitions(topic, kafkaProperties, kafkaFactory));
    }

    public ClusterGroup getReconciliationClusterGroup(Ignite ignite, KafkaFactory kafkaFactory, ReplicaConfig config) {
        String reconciliationTopic = config.getReconciliationTopic();
        return getClusterGroupAsSubcollection(ignite, getNumberOfPartitions(reconciliationTopic,
            config.getConsumerConfig(), kafkaFactory));
    }

    private ClusterGroup getClusterGroupAsSubcollection(Ignite ignite, int size) {
        Collection<ClusterNode> clusterNodes = ignite.cluster().forServers().nodes();

        if (size >= clusterNodes.size()) {
            return ignite.cluster().forServers();
        }
        Iterator<ClusterNode> clusterNodeIter = clusterNodes.iterator();
        List<ClusterNode> groupNodes = new ArrayList<>(size);

        while (groupNodes.size() < size) {
            groupNodes.add(clusterNodeIter.next());
        }
        return ignite.cluster().forNodes(groupNodes);
    }

    private int getNumberOfPartitions(String topic, Properties kafkaProperties, KafkaFactory kafkaFactory) {
        try (Consumer<ByteBuffer, ByteBuffer> consumer = kafkaFactory.consumer(kafkaProperties)) {
            return consumer.partitionsFor(topic).size();
        }
    }
}
