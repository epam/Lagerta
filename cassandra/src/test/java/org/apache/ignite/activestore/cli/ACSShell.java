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

package org.apache.ignite.activestore.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.activestore.CommandService;
import org.mvel2.MVEL;
import org.mvel2.ParserContext;

/**
 * Class which exposes {@link CommandService} API via command line interface.
 */
public class ACSShell {
    /**
     * Default context for code parser usage
     */
    private static final ParserContext PARSER_CONTEXT = new ParserContext() {{
        addPackageImport("java.util");
    }};

    /**
     * Environment variable for ignite configuration file.
     */
    private static final String IGNITE_CONFIG_ENV = "IGNITE_SHELL_CONFIG";

    /**
     * Reads input command and executes it. May refer to {@link CommandService}. Needs running ignite instance.
     *
     * @param args which contain command to execute
     */
    public static void main(String[] args) {
        // Customize System.out in order to suppress Ignite writing banners directly to it.
        PrintStream sysOut = System.out;
        PrintStream sysErr = System.err;
        PrintStream dummyStream = new PrintStream(new OutputStream() {
            public void write(int b) {
                //NO-OP
            }
        });

        System.setOut(dummyStream);
        System.setErr(dummyStream);

        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");
        try {
            if (args.length > 1) {
                sysErr.println("Please specify one command to be executed");
            }
            else if (args.length == 1) {
                cmdMode(sysOut, args[0]);
            }
            else {
                interactiveMode(sysOut);
            }
        }
        catch (Exception e) {
            sysErr.println("Unexpected exception happened: " + e.getMessage());
        }
    }

    /**
     * Starts shell to execute one specified command.
     * @param sysOut to which output will be written.
     * @param expression to be executed.
     */
    private static void cmdMode(PrintStream sysOut, String expression) {
        String igniteConfig = System.getenv(IGNITE_CONFIG_ENV);
        if (igniteConfig == null) {
            sysOut.println("Environment variable " + IGNITE_CONFIG_ENV + " is not set. It should specify ignite " +
                "config file name");
            return;
        }
        try (Ignite ignite = Ignition.start(igniteConfig)) {
            CommandServiceCLIAware serviceCLIAware = new CommandServiceCLIAware(ignite);
            sysOut.println(eval(serviceCLIAware, expression));
        }
    }

    /**
     * Starts shell in interactive mode.
     *
     * @param sysOut to which output will be written.
     * @throws IOException in case of exception during reading commands.
     */
    private static void interactiveMode(PrintStream sysOut) throws IOException {
        sysOut.print("Please enter ignite configuration file name: ");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String configFile = reader.readLine();
            try (Ignite ignite = Ignition.start(configFile)) {
                sysOut.println("Connected to server");
                CommandServiceCLIAware serviceCLIAware = new CommandServiceCLIAware(ignite);
                executeCommandsInteractively(serviceCLIAware, sysOut);
            }
        }
    }

    /**
     * Reads commands from user and executes them.
     * @param serviceCLIAware which provides API.
     * @param sysOut to which output will be written.
     */
    private static void executeCommandsInteractively(CommandServiceCLIAware serviceCLIAware, PrintStream sysOut) {
        sysOut.println("Please write your command and press <enter>");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String expression;
            while (true) {
                expression = reader.readLine();
                if ("exit".equals(expression)) {
                    return;
                }
                try {
                    sysOut.printf("%s\n\n", eval(serviceCLIAware, expression));
                }
                catch (Exception e) {
                    sysOut.println("Unexpected error while executing command: " + e.getMessage());
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes specified command
     *
     * @param serviceCLIAware to be exposed for commands
     * @param expression command which should be executed
     * @return result from requested command of null if it was of void type
     */
    private static Object eval(CommandServiceCLIAware serviceCLIAware, String expression) {
        return MVEL.executeExpression(MVEL.compileExpression(expression, PARSER_CONTEXT), serviceCLIAware);
    }

}
