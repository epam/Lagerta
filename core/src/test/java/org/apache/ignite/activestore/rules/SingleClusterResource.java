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

import org.apache.ignite.activestore.cluster.XmlOneProcessClusterManager;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

/**
 * @author Aleksandr_Meterko
 * @since 12/26/2016
 */
public class SingleClusterResource {
    private static final int PER_TEST_TIMEOUT = 60_000;
    private static final String BASE_DIR = "functional/";
    private static final String CONFIG_XML = "single-dr-cluster-mocked-kafka.xml";

    private static final TestResources CLUSTER_RESOURCE = new TestResources("cluster", 2);

    private static RuleChain allResourcesRule;

    static {
        CLUSTER_RESOURCE.setClusterManager(new XmlOneProcessClusterManager(BASE_DIR + CONFIG_XML));
    }

    public static TestResources getClusterResource() {
        return CLUSTER_RESOURCE;
    }

    public static RuleChain getPerTestMethodRules() {
        return RuleChain
            .outerRule(new Timeout(PER_TEST_TIMEOUT))
            .around(CLUSTER_RESOURCE.perTestMethodCleanupRule());
    }

    public static RuleChain getAllResourcesRule(int timeout) {
        if (allResourcesRule == null) {
            allResourcesRule = RuleChain
                .outerRule(new Timeout(timeout))
                .around(CLUSTER_RESOURCE);
        }
        return allResourcesRule;
    }

}
