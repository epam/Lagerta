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

import com.epam.lathgertha.cluster.AppContextOneProcessClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class FullClusterResource implements Resource {
    private static final Logger LOG = LoggerFactory.getLogger(FullClusterResource.class);
    private static final String CONFIG_XML = "com/epam/lathgertha/integration/config.xml";
    private static final int CLUSTER_SIZE = 2;

    private final DBResource dbResource;

    private final TemporaryDirectory tmpDir = new TemporaryDirectory();
    private final EmbeddedKafka kafka = new EmbeddedKafka(tmpDir, CLUSTER_SIZE, 2181, 9092);
    private final IgniteClusterResource cluster = new IgniteClusterResource(CLUSTER_SIZE,
            new AppContextOneProcessClusterManager(CONFIG_XML));

    public FullClusterResource(DBResource dbResource) {
        this.dbResource = dbResource;
    }

    public IgniteClusterResource igniteCluster() {
        return cluster;
    }

    public DBResource getDBResource() {
        return dbResource;
    }

    @Override
    public void setUp() throws Exception {
        setUpResources(tmpDir, kafka, dbResource, cluster);
    }

    @Override
    public void tearDown() {
        tearDownResources(cluster, kafka, dbResource, tmpDir);
    }

    public void cleanUpClusters() throws SQLException {
        cluster.stopACSServicesAndCaches();
        cluster.startACSServicesAndCaches();
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
}
