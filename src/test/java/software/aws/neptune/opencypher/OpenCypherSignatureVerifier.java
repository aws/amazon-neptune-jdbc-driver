/*
 * Copyright <2023> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.opencypher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * This class can be used to make a query to a real Neptune instance using IAM auth.
 * The query is not supposed to return any result, but if there is a problem with the
 * signature, an exception will be thrown.
 * <p>
 * 1. Run with the parameters --host and --region. If you're using SSH tunneling,
 *    configure your environment as described in markdown/setup/configuration.md,
 *    and include parameters --ssh-file and --ssh-host.
 * 2. Set environment variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and if using
 *    temporary credentials, AWS_SESSION_TOKEN.
 * <p>
 * You can run this class without any parameters to see its usage.
 */
class OpenCypherSignatureVerifier implements Callable<Integer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherSignatureVerifier.class);

    @CommandLine.Option(names = {"--host"}, description = "Neptune hostname")
    private String host;
    @CommandLine.Option(names = {"--region"}, description = "AWS region for Neptune cluster")
    private String region;
    @CommandLine.ArgGroup(exclusive = false, multiplicity = "0..1")
    private Ssh ssh;

    @Override
    public Integer call() {
        final String connString = String.format("jdbc:neptune:opencypher://bolt://%s:8182", host);

        try (Connection connection = DriverManager.getConnection(connString, connectionProperties())) {
            LOGGER.info("Connection created");
            final Statement statement = connection.createStatement();
            LOGGER.info("Statement created");
            final String query = "match (n) where id(n) = '1' return n.prop";
            final ResultSet results = statement.executeQuery(query);
            LOGGER.info("Statement executed");
            while (results.next()) {
                LOGGER.info("Start result");
                final String rd = results.getString(1);
                LOGGER.info(rd);
                LOGGER.info("End result");
            }
        } catch (Exception e) {
            return 1;
        }
        return 0;
    }

    private Properties connectionProperties() {
        final Properties properties = new Properties();
        properties.put("authScheme", "IAMSigV4");
        properties.put("serviceRegion", region);
        properties.put("useEncryption", true);
        properties.put("LogLevel", "DEBUG");

        if (ssh != null) {
            properties.put("sshUser", ssh.user);
            properties.put("sshHost", ssh.host);
            properties.put("sshPrivateKeyFile", ssh.privateKeyFile);
        }

        return properties;
    }

    private static class Ssh {
        @CommandLine.Option(names = {"--ssh-user"}, required = false, description = "username for the internal SSH tunnel")
        private String user = "ec2-user";
        @CommandLine.Option(names = {"--ssh-host"}, description = "host name for the internal SSH tunnel")
        private String host;
        @CommandLine.Option(names = {"--ssh-file"}, description = "path to the private key file for the internal SSH tunnel")
        private String privateKeyFile;
    }

    public static void main(final String[] args) {
        final int exitCode = new CommandLine(new OpenCypherSignatureVerifier()).execute(args);
        System.exit(exitCode);
    }
}
