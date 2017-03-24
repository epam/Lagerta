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

package org.apache.ignite.load;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.injection.InjectionForTests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/23/2017 3:41 PM
 */
public final class LoadGeneratingClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadGeneratingClient.class);

    private LoadGeneratingClient() {
    }

    public static void main(String[] args) throws Exception {
        try (Ignite ignite = TestsHelper.getClusterClient(args)) {
            LOGGER.info("[T] Load test execution started");
            InjectionForTests.get(LoadTestDriver.class, ignite).run();
            LOGGER.info("[T] Load test execution completed");
        }
    }
}
