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

package org.apache.ignite.activestore.load;

import org.apache.ignite.activestore.utils.CassandraHelper;
import org.apache.log4j.Logger;

/**
 * Used to manually clean up Cassandra cluster after load test execution.
 */
public class LoadTestsCassandraCleaner {
    /** */
    private static final Logger LOGGER = Logger.getLogger(LoadTestsCassandraCleaner.class);

    /** */
    public static void main(String[] args) {
        try {
            LOGGER.info("Dropping all artifacts from previous tests execution session");
            CassandraHelper.dropTestKeyspaces();
        }
        finally {
            CassandraHelper.releaseCassandraResources();
        }
    }
}
