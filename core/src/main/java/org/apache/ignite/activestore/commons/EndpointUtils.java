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

import javax.xml.ws.Endpoint;

/**
 * @author Evgeniy_Ignatiev
 * @since 15:19 01/25/2017
 */
public final class EndpointUtils {
    private static final String ENDPOINT_ADDRESS_PATTERN = "http://%s/ws/%s";

    private EndpointUtils() {
    }

    public static Endpoint publish(String address, String serviceName, Object service) {
        return Endpoint.publish(formatEndpoint(address, serviceName), service);
    }

    public static String formatEndpoint(String address, String serviceName) {
        return String.format(ENDPOINT_ADDRESS_PATTERN, address, serviceName);
    }
}
