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

package com.epam.lathgertha.resources;

import com.epam.lathgertha.cluster.IgniteClusterManager;
import com.epam.lathgertha.cluster.XmlOneProcessClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationClusters implements Resource {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationClusters.class);
    private static final String CONFIGS_DIR = "com/epam/lathgertha/integration/";
    private static final String MAIN_CLUSTER_CONFIG_XML = "ignite-main-cluster-config.xml";
    private static final String READER_CLUSTER_CONFIG_XML = "ignite-reader-cluster-config.xml";
    private static final int CLUSTER_SIZE = 2;

    private final TemporaryDirectory tmpDir = new TemporaryDirectory();
    private final EmbeddedKafka mainKafka = new EmbeddedKafka(tmpDir, CLUSTER_SIZE, 2181, 9092);
    private final EmbeddedKafka readerKafka = new EmbeddedKafka(tmpDir, CLUSTER_SIZE, 2182, 9096);
    private final IgniteClusterResource mainCluster = createCluster(MAIN_CLUSTER_CONFIG_XML, CLUSTER_SIZE);
    private final IgniteClusterResource readerCluster = createCluster(READER_CLUSTER_CONFIG_XML, CLUSTER_SIZE);

    public IgniteClusterResource mainCluster() {
        return mainCluster;
    }

    public IgniteClusterResource readerCluster() {
        return readerCluster;
    }

    @Override
    public void setUp() throws Exception {
        setUpResources(tmpDir, mainKafka, readerKafka, mainCluster, readerCluster);
    }

    @Override
    public void tearDown() {
        tearDownResources(readerCluster, mainCluster, mainKafka, readerKafka, tmpDir);
    }

    public void cleanUpClusters() {
        mainCluster.clearCluster();
        readerCluster.clearCluster();
        mainKafka.deleteAllTopics();
        readerKafka.deleteAllTopics();
    }

    private static void setUpResources(Resource... resources) throws Exception {
        for (Resource resource : resources) {
            resource.setUp();
        }
    }

    private static void tearDownResources(Resource... resources) {
        for (Resource resource : resources) {
            try {
                resource.tearDown();
            } catch (Exception e) {
                LOG.error("Exception occurred while releasing resources: ", e);
            }
        }
    }

    private static IgniteClusterResource createCluster(String configFile, int clusterSize) {
        IgniteClusterManager clusterManager = new XmlOneProcessClusterManager(getConfigClassPath(configFile));
        return new IgniteClusterResource(clusterSize, clusterManager);
    }

    private static String getConfigClassPath(String configFile) {
        return CONFIGS_DIR + configFile;
    }
}
