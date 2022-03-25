/*
 * Copyright <2022> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package sample.applications;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This class provides an example on how to create a {@link Connection} with some extra config options.
 */
public class ConnectionOptionsExamples {
    /**
     * This function creates a {@link Connection} with additional config options.
     *
     * @return {@link Connection} with additional config options.
     * @throws SQLException If creating the {@link Connection} Object fails or is not valid.
     */
    public static Connection createConnectionWithExtraConfigs() throws SQLException {
        // To enable IAM SigV4 authentication, set the AuthScheme to IAMSigV4 in the Properties map.
        final Properties properties = new Properties();

        // To enable encryption, set the UseEncryption to true in the Properties map.
        properties.put("UseEncryption", "true");

        // To set the connection timeout, set the ConnectionTimeout to an integer valued string (milliseconds) in the Properties map.
        properties.put("ConnectionTimeout", "1000");

        // To set the connection retry count, set the ConnectionRetryCount to an integer valued string in the Properties map.
        properties.put("ConnectionRetryCount", "3");

        // To set the connection pool size, set the ConnectionPoolSize to an integer valued string in the Properties map.
        properties.put("ConnectionPoolSize", "1000");

        // To set the log level, set the LogLevel to a string value in the Properties map.
        properties.put("LogLevel", "DEBUG");

        final Connection connection = DriverManager.getConnection(ExampleConfigs.getConnectionString(), properties);

        // Validate the connection with a timeout of 5 seconds.
        // If this line fails, create a {@link java.sql.Statement} and execute a literal query to check the error reported there.
        if (!connection.isValid(5)) {
            throw new SQLException(
                    "Connection is not valid, verify that the url, port, and credentials are set correctly.");
        }
        return connection;
    }
}
