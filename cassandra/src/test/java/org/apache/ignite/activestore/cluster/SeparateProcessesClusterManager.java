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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

import static org.apache.ignite.activestore.cluster.IgniteConfigHelper.setNumberProcesses;

/**
 * Implementation of {@link IgniteClusterManager} which starts all grids in separate processes.
 */
public class SeparateProcessesClusterManager implements IgniteClusterManager {
    /**
     * Spawned processes.
     */
    private List<Process> igniteProcesses = new ArrayList<>();
    /**
     * Grid in current process.
     */
    private Ignite root;

    /** {@inheritDoc} */
    @Override public Ignite startCluster(int nodes, IgniteConfiguration igniteConfiguration) {
        IgniteConfiguration configuration = new IgniteConfiguration(igniteConfiguration);
        configuration.setGridName("root");
        setNumberProcesses(configuration, nodes);
        root = Ignition.start(configuration);

        throw new UnsupportedOperationException();
//        for (int i = 1; i < nodes; i++) {
//            igniteProcesses.add(startServerProcess(nodes));
//        }
//        waitTillClusterStart(nodes);
//        return root;
    }

    /**
     * Waits until all nodes were started.
     *
     * @param nodes number of nodes in cluster.
     */
    private void waitTillClusterStart(int nodes) {
        do {
            try {
                Thread.sleep(5000);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        while (startedNodes() != nodes);
    }

    /**
     * Returns number of started nodes in cluster.
     *
     * @return nodes.
     */
    private int startedNodes() {
        long currentVersion = root.cluster().topologyVersion();
        return root.cluster().topology(currentVersion).size();
    }

    /** {@inheritDoc} */
    @Override public void stopCluster() {
        root.cluster().stopNodes();
        for (Process process : igniteProcesses) {
            try {
                process.waitFor();
            }
            catch (InterruptedException e) {
                throw new RuntimeException("Exception while waiting for nodes to stop", e);
            }
        }
    }

    /**
     * Starts ignite grid in separate process.
     *
     * @param processes overall number of planned grids in cluster.
     * @return spawned process.
     */
    private Process startServerProcess(final int processes) {
        String fileSeparator = File.separator;
        final String path = System.getProperty("java.home") + fileSeparator + "bin" + fileSeparator + "java";

        List<String> command = new ArrayList<String>() {{
            add(path);
            add("-cp");
            add(getClassPath(System.getProperty("java.class.path")));
            add(IgniteConfigHelper.class.getName());
            add(Integer.toString(processes));
        }};
        ProcessBuilder builder = new ProcessBuilder(command);
        try {
            return builder.start();
        }
        catch (IOException e) {
            throw new RuntimeException("Could not start node", e);
        }
    }

    /**
     * Returns class path to be used for process start.
     *
     * @param classPath full class path.
     * @return filtered class path.
     */
    private String getClassPath(String classPath) {
        StringBuilder sb = new StringBuilder();
        for (String jar : classPath.split(File.pathSeparator)) {
            if (!jar.contains("idea")) {
                sb.append(jar).append(File.pathSeparator);
            }
        }
        return sb.toString();
    }

}
