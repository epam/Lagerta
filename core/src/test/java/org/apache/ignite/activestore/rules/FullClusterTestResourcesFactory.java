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

import java.nio.file.Paths;

import org.apache.ignite.activestore.cluster.XmlOneProcessClusterManager;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * @author Aleksandr_Meterko
 * @since 12/14/2016
 */
public class FullClusterTestResourcesFactory {
    private static final int PER_TEST_TIMEOUT = 2 * 60_000;
    private static final String REPLICATION_DIR = "replication/";
    private static final String MAIN_CLUSTER_CONFIG_XML = "ignite-main-cluster-config.xml";
    private static final String DR_CLUSTER_CONFIG_XML = "ignite-dr-cluster-config.xml";

    private static final SuperClusterResource SUPER_CLUSTER_RESOURCE = new SuperClusterResource("superCuster");
    private static final TestResources MAIN_CLUSTER_RESOURCE = new TestResources("mainCluster", 2);
    private static final TestResources DR_CLUSTER_RESOURCE = new TestResources("drCluster", 2);
    private static final TemporaryFolder FOLDER = new TemporaryFolder(Paths.get("").toAbsolutePath().toFile());

    private static RuleChain allResourcesRule;

    static {
        XmlOneProcessClusterManager mainManager = new XmlOneProcessClusterManager(getConfigClassPath(MAIN_CLUSTER_CONFIG_XML));
        FullClusterTestResourcesFactory.getMainClusterResource().setClusterManager(mainManager);
        XmlOneProcessClusterManager drManager = new XmlOneProcessClusterManager(getConfigClassPath(DR_CLUSTER_CONFIG_XML));
        FullClusterTestResourcesFactory.getDrClusterResource().setClusterManager(drManager);
    }

    public static TestResources getMainClusterResource() {
        return MAIN_CLUSTER_RESOURCE;
    }

    public static TestResources getDrClusterResource() {
        return DR_CLUSTER_RESOURCE;
    }

    public static RuleChain getPerTestMethodRules() {
        return RuleChain
            .outerRule(new Timeout(PER_TEST_TIMEOUT))
            .around(MAIN_CLUSTER_RESOURCE.perTestMethodCleanupRule())
            .around(DR_CLUSTER_RESOURCE.perTestMethodCleanupRule());
    }

    public static RuleChain getAllResourcesRule(int timeout) {
        if (allResourcesRule == null) {
            allResourcesRule = RuleChain
                .outerRule(FOLDER)
                .around(new Timeout(timeout))
                .around(new EmbeddedKafkaRule(FOLDER, "mainKafka", 3, 2181, 9092))
                .around(new EmbeddedKafkaRule(FOLDER, "drKafka", 3, 2182, 9096))
                .around(SUPER_CLUSTER_RESOURCE)
                .around(MAIN_CLUSTER_RESOURCE)
                .around(DR_CLUSTER_RESOURCE);
        }
        return allResourcesRule;
    }

    private static String getConfigClassPath(String configFile) {
        return REPLICATION_DIR + configFile;
    }
}
