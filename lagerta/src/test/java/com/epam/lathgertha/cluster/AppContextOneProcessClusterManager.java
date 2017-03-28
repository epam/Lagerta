/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lathgertha.cluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.ArrayList;
import java.util.List;

public class AppContextOneProcessClusterManager extends DefaultOneProcessClusterManager {
    private final String configPath;
    private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();

    public AppContextOneProcessClusterManager(String configPath) {
        this.configPath = configPath;
    }

    /** {@inheritDoc} */
    @Override
    protected Ignite startGrid(int gridNumber, int clusterSize) {
        try {
            ConfigurableApplicationContext applicationContext = new ClassPathXmlApplicationContext(configPath);
            contexts.add(applicationContext);
            IgniteConfiguration config = applicationContext.getBean(IgniteConfiguration.class);
            return IgniteSpring.start(config, applicationContext);
        } catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    public void refreshContexts() {
        contexts.forEach(ConfigurableApplicationContext::refresh);
    }
}
