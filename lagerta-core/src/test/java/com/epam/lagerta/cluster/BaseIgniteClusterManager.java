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

package com.epam.lagerta.cluster;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.services.ServiceConfiguration;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class BaseIgniteClusterManager implements IgniteClusterManager {
    protected Ignite clientNode;
    protected List<CacheConfiguration> cacheConfigs;
    protected ServiceConfiguration[] serviceConfigs;

    protected void stopServicesAndCaches() {
        do {
            cacheConfigs.forEach(config -> clientNode.destroyCache(config.getName()));
            if (serviceConfigs != null) {
                clientNode.services().cancelAll();
            }
            Uninterruptibles.sleepUninterruptibly(AWAIT_TIME, TimeUnit.MILLISECONDS);
        } while (clientNode.cacheNames().size() != 0);
    }

    protected void startServicesAndCaches() {
        clientNode.createCaches(cacheConfigs);
        if (serviceConfigs != null) {
            IgniteServices services = clientNode.services();
            Arrays.stream(serviceConfigs).forEach(services::deploy);
        }
        Uninterruptibles.sleepUninterruptibly(AWAIT_TIME, TimeUnit.MILLISECONDS);
    }

    protected List<CacheConfiguration> getNonSystemCacheConfigs() {
        return Arrays
                .stream(clientNode.configuration().getCacheConfiguration())
                .filter(config -> !GridCacheUtils.isSystemCache(config.getName()))
                .collect(Collectors.toList());
    }
}
