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
import org.apache.ignite.IgniteCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.GenericXmlApplicationContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DifferentJVMClusterManager extends BaseIgniteClusterManager {

    private static final Logger LOG = LoggerFactory.getLogger(DifferentJVMClusterManager.class);
    private static final String CONFIG_XML = "com/epam/lagerta/integration/client-config.xml";

    private IgniteStopper igniteStopper;
    private List<Process> processes;
    private int clusterSize = 0;

    @Override
    public Ignite startCluster(int clusterSize) {
        this.clusterSize = clusterSize;
        clientNode = new GenericXmlApplicationContext(CONFIG_XML).getBean(Ignite.class);
        processes = new ArrayList<>(clusterSize);
        startServerNodes(clusterSize);
        igniteStopper = new IgniteStopper(clientNode);
        cacheConfigs = getNonSystemCacheConfigs();
        serviceConfigs = clientNode.configuration().getServiceConfiguration();
        return clientNode;
    }

    public void startServerNodes(int count) {
        processes = processes.stream().filter(Process::isAlive).collect(Collectors.toList());
        try {
            for (int gridNumber = 0; gridNumber < count && processes.size() < clusterSize; gridNumber++) {
                processes.add(startJVM("node-" + (processes.size() + gridNumber), IgniteStarter.class));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stopCluster() {
        if (igniteStopper != null) {
            igniteStopper.stopAllServerNodes();
        }
        processes.stream().filter(Process::isAlive).forEach(Process::destroy);
        clientNode.close();
    }

    @Override
    public void reloadCluster() {
        startServerNodes(clusterSize);
        waitStartServerNodes();
        stopServicesAndCaches();
        startServicesAndCaches();
    }

    public IgniteStopper getIgniteStopper() {
        return igniteStopper;
    }

    public long getCountAliveServerNodes() {
        return processes.stream().filter(Process::isAlive).count();
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
            Thread logThread = new Thread(() -> {
                String out = null;
                try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream))) {
                    while ((out = bufferedReader.readLine()) != null) {
                        LOG.info("[ {} ] {}", processName, out);
                    }
                } catch (IOException e) {
                    LOG.error("Error output: ", e);
                }
            });
            logThread.setDaemon(true);
            logThread.start();
        }
    }

    public void waitStartServerNodes() {
        IgniteCluster cluster = clientNode.cluster();
        do {
            Uninterruptibles.sleepUninterruptibly(AWAIT_TIME, TimeUnit.MILLISECONDS);
        } while (cluster.forServers().nodes().size() <= clusterSize);
    }
}
