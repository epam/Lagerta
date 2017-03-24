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

package org.apache.ignite.activestore.cluster;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Helper class which enriches {@link IgniteConfiguration} with additional data.
 */
public class IgniteConfigHelper {
    /**
     * Configures discovery options based by number of used grid instances.
     *
     * @param configuration base configuration.
     * @param numberOfProcesses which will be used to run test suite.
     */
    public static void setNumberProcesses(IgniteConfiguration configuration, int numberOfProcesses) {
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (int i = 0; i < numberOfProcesses; i++) {
            addresses.add(InetSocketAddress.createUnresolved("127.0.0.1", 47500 + i));
        }
        finder.registerAddresses(addresses);
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(finder);
        configuration.setDiscoverySpi(discoverySpi);
    }
}
