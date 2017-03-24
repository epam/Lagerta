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

package org.apache.ignite.activestore.impl.config;

import org.apache.ignite.activestore.commons.EndpointUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;

/**
 * @author Andrei_Yakushin
 * @since 1/17/2017 11:41 AM
 */

@WebService(endpointInterface = "org.apache.ignite.activestore.impl.config.SuperCluster")
public class SimpleSuperCluster implements SuperCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSuperCluster.class);

    private String main;

    @Override
    public String getMain(String address) {
        LOGGER.info("[G] New cluster registered {}", address);
        return main == null ? (main = address) : main;
    }

    public static Endpoint publish(String address) {
        return EndpointUtils.publish(address, NAME, new SimpleSuperCluster());
    }
}
