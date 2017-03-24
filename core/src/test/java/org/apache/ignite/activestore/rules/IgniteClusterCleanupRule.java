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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.activestore.commons.injection.Injection;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.ServiceConfiguration;
import org.junit.rules.ExternalResource;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/17/2017 6:24 PM
 */
public class IgniteClusterCleanupRule extends ExternalResource {
    private final Ignite ignite;
    private final List<CacheConfiguration> cacheConfigs;
    private final List<ServiceConfiguration> serviceConfigs;

    public IgniteClusterCleanupRule(Ignite ignite) {
        this.ignite = ignite;
        cacheConfigs = getNonSystemCacheConfigs();
        serviceConfigs = Arrays.asList(ignite.configuration().getServiceConfiguration());
    }

    private List<CacheConfiguration> getNonSystemCacheConfigs() {
        List<CacheConfiguration> configs = new ArrayList<>();
        for (CacheConfiguration config : ignite.configuration().getCacheConfiguration()) {
            if (!GridCacheUtils.isSystemCache(config.getName())) {
                configs.add(config);
            }
        }
        return configs;
    }

    // Cleaning up in after leads to tests hanging in the end and will still leave us with the one useless cleanup.
    @Override protected void before() {
        IgniteServices services = ignite.services();

        services.cancelAll();
        destroyCaches();
        ignite.compute().broadcast(new InjectionStopTask());
        ignite.createCaches(cacheConfigs);
        deployServices(services);
        try {
            Thread.sleep(5_000);
        } catch (InterruptedException e) {
            // Do nothing.
        }
    }

    private void destroyCaches() {
        for (CacheConfiguration config : cacheConfigs) {
            ignite.destroyCache(config.getName());
        }
    }

    private void deployServices(IgniteServices services) {
        for (ServiceConfiguration config : serviceConfigs) {
            services.deploy(config);
        }
    }

    private static class InjectionStopTask implements IgniteRunnable {
        @IgniteInstanceResource
        private transient Ignite ignite;

        @Override public void run() {
            Injection.stop(ignite);
        }
    }
}
