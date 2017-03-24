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

import java.io.IOException;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.injection.InjectionForTests;
import org.apache.ignite.load.statistics.StatisticsDriver;
import org.apache.ignite.load.statistics.reporters.ReportersManager;

/**
 * After the ignite cluster has started, this driver can be executed to start statistics reporting on the each node of
 * the cluster, aggregate cluster-wide performance statistics and stop the cluster after the node overload statistics
 * reporter reports each of the cluster node as overloaded.
 */
public class LoadTestsStatisticsDriver {
    public static void main(String[] args) throws IOException {
        try (Ignite ignite = TestsHelper.getClusterClient(args)) {
            StatisticsDriver statistics = InjectionForTests.get(StatisticsDriver.class, ignite);
            ReportersManager reportersManager = InjectionForTests.get(ReportersManager.class, ignite);
            LoadTestDriversCoordinator coordinator = InjectionForTests.get(LoadTestDriversCoordinator.class, ignite);

            reportersManager.startReporters();
            statistics.startCollecting();
            coordinator.startCoordinator();
            coordinator.awaitCoordinatorStop();
        }
    }
}
