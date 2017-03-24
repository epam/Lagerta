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

import java.io.IOException;
import java.nio.file.Paths;
import org.apache.ignite.activestore.impl.util.FileUtils;
import org.apache.ignite.activestore.utils.CassandraHelper;
import org.apache.log4j.Logger;
import org.junit.rules.ExternalResource;

/**
 * Allows to start and stop cassandra.
 */
public class CassandraRule extends ExternalResource {
    /** */
    private static final Logger LOGGER = Logger.getLogger(CassandraRule.class.getName());
    /** */
    private static final String CASSANDRA_DIR = "data";
    /** */
    private boolean forceEmbedded;

    /** */
    public CassandraRule(boolean forceEmbedded) {
        this.forceEmbedded = forceEmbedded;
    }

    /** */
    public static void start(boolean forceEmbedded) throws IOException {
        FileUtils.deleteDir(Paths.get(CASSANDRA_DIR));

        if (forceEmbedded || CassandraHelper.useEmbeddedCassandra()) {
            try {
                CassandraHelper.startEmbeddedCassandra(LOGGER);
            }
            catch (Throwable e) {
                throw new RuntimeException("Failed to start embedded Cassandra instance", e);
            }
        }
        LOGGER.info("Testing admin connection to Cassandra");
        CassandraHelper.testAdminConnection();
        LOGGER.info("Testing regular connection to Cassandra");
        CassandraHelper.testRegularConnection();
        LOGGER.info("Dropping all artifacts from previous tests execution session");
        CassandraHelper.dropTestKeyspaces();
        LOGGER.info("Start tests execution");
    }

    /** {@inheritDoc} */
    @Override protected void before() throws Throwable {
        start(forceEmbedded);
    }

    /** */
    public static void stop(final boolean forceEmbedded) {
        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                CassandraHelper.releaseCassandraResources();
                if (forceEmbedded || CassandraHelper.useEmbeddedCassandra()) {
                    try {
                        CassandraHelper.stopEmbeddedCassandra();
                    }
                    catch (Throwable e) {
                        LOGGER.error("Failed to stop embedded Cassandra instance", e);
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    /** {@inheritDoc} */
    @Override protected void after() {
        stop(forceEmbedded);
    }
}
