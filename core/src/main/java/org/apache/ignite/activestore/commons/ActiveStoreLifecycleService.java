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

package org.apache.ignite.activestore.commons;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.injection.Injection;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/20/2016 3:14 PM
 */
public class ActiveStoreLifecycleService implements Service {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveStoreLifecycleService.class);

    public static final String SERVICE_NAME = "activeStoreLifecycleService";

    @IgniteInstanceResource
    private transient Ignite ignite;

    private transient boolean running;

    @Override public void init(ServiceContext context) {
        running = true;
    }

    @Override public void execute(ServiceContext context) {
        // Do nothing.
    }

    @Override public void cancel(ServiceContext context) {
        if (running) {
            LOGGER.debug("[G] Stopping service");
            Injection.stop(ignite);
            running = false;
        }
    }
}
