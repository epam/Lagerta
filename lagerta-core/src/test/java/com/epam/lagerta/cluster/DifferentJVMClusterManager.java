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

import com.epam.lagerta.IgniteConfigurer;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class DifferentJVMClusterManager implements IgniteClusterManager {

    private static final Logger LOG = LoggerFactory.getLogger(DifferentJVMClusterManager.class);

    private static final String CLIENT_GRID_NAME = "clientNode";
    private List<Process> processes = new ArrayList<>();
    private Ignite clientNode;
    private IgniteStopper igniteStopper;

    @Override
    public Ignite startCluster(int clusterSize) {
        try {
            for (int gridNumber = 0; gridNumber < clusterSize; gridNumber++) {
                processes.add(startJVM("node-" + gridNumber, IgniteStarter.class));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        clientNode = Ignition.start(IgniteConfigurer.getIgniteConfiguration(CLIENT_GRID_NAME, true));
        igniteStopper = new IgniteStopper(clientNode);
        return clientNode;
    }

    @Override
    public void stopCluster() {
        igniteStopper.stopAllServerNodes();
        clientNode.close();
    }

    public IgniteStopper getIgniteStopper() {
        return igniteStopper;
    }

    private Process startJVM(String gridName, Class classForRun) throws IOException, InterruptedException {
        List<String> params = new ArrayList<String>();
        params.add("java");
        params.add("-cp");
        params.add(System.getProperty("java.class.path"));
        params.add(classForRun.getName());

        ProcessBuilder builderExecute = new ProcessBuilder(params);

        Process process = builderExecute.start();
        InputStream stderr = process.getErrorStream();
        InputStream stdout = process.getInputStream();

        printOutputProcess(gridName, stdout, stderr);
        return process;
    }

    private void printOutputProcess(String processName, InputStream... inputStreams) {
        for (InputStream stream : inputStreams) {
            new Thread(() -> {
                String out = null;
                try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream))) {
                    while ((out = bufferedReader.readLine()) != null) {
                        LOG.info("[ {} ] {}", processName, out);
                    }
                } catch (IOException e) {
                    LOG.error("Error output: ", e);
                }
            }).start();
        }
    }
}
