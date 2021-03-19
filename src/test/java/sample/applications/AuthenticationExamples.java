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
 */

package sample.applications;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This class provides some examples on how to create a {@link Connection} with authentication for Amazon Neptune.
 */
public class AuthenticationExamples {
    /**
     * This function creates a {@link Connection} with IAM SigV4 authentication.
     *
     * @return {@link Connection} with IAM SigV4.
     * @throws SQLException If creating the {@link Connection} Object fails or is not valid.
     */
    public static Connection createIAMAuthConnection() throws SQLException {
        // To enable IAM SigV4 authentication, set the AuthScheme to IAMSigV4 in the Properties map.
        final Properties properties = new Properties();
        properties.put("AuthScheme", "IAMSigV4");

        // Create the connection. IAM SigV4 authentication requires that the environment of the user is setup to
        // receive credentials. Notice, the SERVICE_REGION must be set appropriately as well. See
        // https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-gremlin-java.html for more information.

        // On Windows this can be done by settings the AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and SERVICE_REGION environment variables.
        // On Mac a ~/.aws file can be created and populated with the credentials.

        // If TFA is setup on the account, follow the appropriate steps to set the session token for your environment as well.
        final Connection connection = DriverManager.getConnection(ExampleConfigs.getConnectionString(), properties);

        // Validate the connection with a timeout of 5 seconds.
        // If this line fails, create a {@link java.sql.Statement} and execute a literal query to check the error reported there.
        if (!connection.isValid(5)) {
            throw new SQLException("Connection is not valid, verify that the url, port, and credentials are set correctly.");
        }
        return connection;
    }

    /**
     * This function creates a {@link Connection} without authentication.
     *
     * @return {@link Connection} without authentication.
     * @throws SQLException If creating the {@link Connection} Object fails or is not valid.
     */
    public static Connection createNoAuthConnection() throws SQLException {
        // To create a connection without authentication, set AuthScheme to None in the Properties map.
        final Properties properties = new Properties();
        properties.put("AuthScheme", "None");
        final Connection connection = DriverManager.getConnection(ExampleConfigs.getConnectionString(), properties);

        // Validate the connection with a timeout of 5 seconds.
        // If this line fails, create a {@link java.sql.Statement} and execute a literal query to check the error reported there.
        if (!connection.isValid(5)) {
            throw new SQLException("Connection is not valid, verify that the url and port are set correctly.");
        }
        return connection;
    }
}
