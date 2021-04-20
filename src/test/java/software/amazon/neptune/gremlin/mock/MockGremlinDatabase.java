/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.neptune.gremlin.mock;

import org.apache.commons.lang3.SystemUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;

public class MockGremlinDatabase {
    private static final String WINDOWS_EXT = ".bat";
    private static final String NIX_EXT = ".sh";
    private static final String SERVER_COMMAND =
            String.format(
                    "./gremlin-server/target/apache-tinkerpop-gremlin-server-3.5.0-SNAPSHOT-standalone/bin/gremlin-server%s",
                    SystemUtils.IS_OS_WINDOWS ? WINDOWS_EXT : NIX_EXT);
    private static final String START_COMMAND = String.format("%s start", SERVER_COMMAND);
    private static final String STOP_COMMAND = String.format("%s stop", SERVER_COMMAND);


    /**
     * Simple function to start the database.
     *
     * @throws IOException thrown if command fails.
     * @throws InterruptedException thrown if command fails.
     */
    public static void startGraph() throws IOException, InterruptedException {
        final String output = runCommand(START_COMMAND);
        if (output.startsWith("Server already running with PID")) {
            stopGraph();
            startGraph();
        }
        System.out.println("Time1: " + Instant.now().toEpochMilli());
        Thread.sleep(5000);
        System.out.println("Time2: " + Instant.now().toEpochMilli());
    }

    /**
     * Simple function to shut the database down.
     *
     * @throws IOException thrown if command fails.
     */
    public static void stopGraph() throws IOException {
        runCommand(STOP_COMMAND);
    }

    private static String runCommand(final String command) throws IOException {
        final Process p = Runtime.getRuntime().exec(command);
        final BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
        final String line = input.readLine();
        System.out.println("Server output: '" + line + "'.");
        return line;
    }
}
