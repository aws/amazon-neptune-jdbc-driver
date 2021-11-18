/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.aws.neptune.gremlin.mock;

import org.apache.commons.lang3.SystemUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class MockGremlinDatabase {
    private static final String WINDOWS_SERVER = "gremlin-server.bat";
    private static final String NIX_SERVER = "gremlin-server.sh";
    private static final String SERVER_PATH =
            "./gremlin-server/target/apache-tinkerpop-gremlin-server-3.5.0-SNAPSHOT-standalone/bin/";
    private static final String SERVER_COMMAND = SERVER_PATH + NIX_SERVER;
    private static final String START_COMMAND = String.format("%s start", SERVER_COMMAND);
    private static final String STOP_COMMAND = String.format("%s stop", SERVER_COMMAND);
    private static Process serverProcess = null;


    /**
     * Simple function to start the database.
     *
     * @throws IOException          thrown if command fails.
     * @throws InterruptedException thrown if command fails.
     */
    public static void startGraph() throws IOException, InterruptedException {
        final String output = runCommand(START_COMMAND);
        if (output.startsWith("Server already running with PID")) {
            return;
        }
        Thread.sleep(10000);
    }

    /**
     * Simple function to shut the database down.
     *
     * @throws IOException thrown if command fails.
     */
    public static void stopGraph() throws IOException, InterruptedException {
        runCommand(STOP_COMMAND);
        Thread.sleep(10000);
    }

    private static String runCommand(final String command) throws IOException {
        if (SystemUtils.IS_OS_WINDOWS) {
            runWindowsCommand(command);
        } else {
            serverProcess = Runtime.getRuntime().exec(command);
        }

        final BufferedReader input = new BufferedReader(new InputStreamReader(serverProcess.getInputStream()));
        return input.readLine();
    }

    private static void runWindowsCommand(final String command) throws IOException {
        if (command.equals(START_COMMAND)) {
            final ProcessBuilder pb = new ProcessBuilder("cmd", "/c", WINDOWS_SERVER);
            final File directoryFile = new File(SERVER_PATH);
            pb.directory(directoryFile);
            serverProcess = pb.start();
        }

        if (command.equals(STOP_COMMAND) && serverProcess != null) {
            serverProcess.destroy();
        }
    }
}
