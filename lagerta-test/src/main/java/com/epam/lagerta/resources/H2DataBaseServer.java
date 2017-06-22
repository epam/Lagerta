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

package com.epam.lagerta.resources;

import org.h2.tools.Server;

public class H2DataBaseServer implements Resource {

    public static final String PORT = "9123";
    public static final String HOST = "localhost";

    private Server server;

    @Override
    public void setUp() throws Exception {
        server = Server.createTcpServer(
                "-tcpPort", PORT, "-tcpAllowOthers", "-tcpDaemon");
        server.start();
    }

    @Override
    public void tearDown() throws Exception {
        server.stop();
    }
}
