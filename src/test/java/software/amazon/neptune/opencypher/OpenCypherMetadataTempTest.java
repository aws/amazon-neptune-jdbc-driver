/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.neptune.opencypher;

import org.junit.jupiter.api.Test;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class OpenCypherMetadataTempTest {

    void runProcess(final String command) {
        /*
        final ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(command);
        processBuilder.inheritIO();
        processBuilder.directory(new File("/Users/lyndonb/repos/neptunejdbc/amazon-neptune-tools/neptune-export/target"));

         */
        final StringBuilder output = new StringBuilder();
        System.out.println("Executing: '" + command + "'.");
        try {
            final Process process = Runtime.getRuntime().exec(command);
            // final Process process = processBuilder.start();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }

            final int exitVal = process.waitFor();
            if (exitVal == 0) {
                System.out.println("Success!");
                System.out.println(output);
            } else {
                System.out.println("Failure!");
                System.out.println(output);
            }
        } catch (final IOException | InterruptedException e) {
            e.printStackTrace();
            System.out.println(output);
        }
    }

    @Test
    void test() throws InterruptedException {
        runProcess("java -jar ./amazon-neptune-tools/neptune-export/target/neptune-export.jar");// create-pg-config");
        // System.out.println("Port: " + database.getPort());
        Thread.sleep(30 * 1000);
        final String config = String.format("%s", "create-pg-config");
        final String endpoint = String.format("-e %s", "localhost"); //database.getPort()));
        final String outputDirectory = String.format("-d %s", ".");
        final String user = String.format("-nl %s", "User");
        final String follows = String.format("-el %s", "FOLLOWS");
        // final String port = String.format("-lb-port %s", database.getPort());
        final String stringBuilder = "java -jar ./amazon-neptune-tools/neptune-export/target/neptune-export.jar" +
                " " + config +
                " " + endpoint +
                " " + outputDirectory +
                " " + user +
                " " + follows;
        // " " + port;
        runProcess(stringBuilder);

        runProcess("java -jar");
        runProcess("java -jar ./neptune-export.jar");
        runProcess("cd /Users/lyndonb/repos/neptunejdbc && pwd");

        // runProcess("pwd && pushd ./amazon-neptune-tools && pwd && popd");
    }


    /*
    @Test
    void testGetTables() throws SQLException, IOException {
        final String runJar = String.format("%s", "java -jar ./amazon-neptune-tools/neptune-export/target/neptune-export.jar");
        final String jar =                        "java -jar ./amazon-neptune-tools/neptune-export/target/neptune-export.jar";
        // create-pg-config -e localhost:1111 -d . -nl User -el FOLLOWS
        final String config = String.format("%s", "create-pg-config");
        final String endpoint = String.format("-e %s:%s", HOSTNAM, 1000); //database.getPort()));
        final String outputDirectory = String.format("-d %s", ".");
        final String user = String.format("-nl %s", "User");
        final String follows = String.format("-el %s", "FOLLOWS");
        runProcess(runJar);
        runProcess(runJar + " " + config);
        final String command =
                String.format("%s %s %s %s %s %s", runJar, config, endpoint, outputDirectory, user, follows);
        System.out.println("Command: " + command);
        runProcess(command);
        // databaseMetaData.getTables(null, null, null, null);
    }*/
}
