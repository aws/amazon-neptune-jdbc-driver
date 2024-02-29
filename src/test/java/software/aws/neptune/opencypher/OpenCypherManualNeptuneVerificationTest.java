/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.opencypher.utilities.OpenCypherGetColumnUtilities;
import java.sql.SQLException;
import java.util.Properties;

import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_HOSTNAME;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_PRIVATE_KEY_FILE;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_STRICT_HOST_KEY_CHECKING;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_USER;

@Disabled
public class OpenCypherManualNeptuneVerificationTest {

    // Before starting manual tests, modify parameters to your specific cluster and SSH tunnel
    private static final String HOSTNAME = "neptune-cluster-url.cluster-xxxxxxxxx.mock-region-1.neptune.amazonaws.com";
    private static final Properties PROPERTIES = new Properties();
    private static final String CREATE_NODES;
    private static java.sql.Connection connection;
    private static java.sql.DatabaseMetaData databaseMetaData;

    static {
        CREATE_NODES = String.format("CREATE (:%s %s)", "Person:Developer", "{hello:'world'}") +
                String.format(" CREATE (:%s %s)", "Person", "{person:1234}") +
                String.format(" CREATE (:%s %s)", "Human", "{hello:'world'}") +
                String.format(" CREATE (:%s %s)", "Human", "{hello:123}") +
                String.format(" CREATE (:%s %s)", "Developer", "{hello:123}") +
                // String.format(" CREATE (:%s %s)", "Person", "{person:'1234'}") +
                String.format(" CREATE (:%s %s)", "Person", "{p1:true}") +
                String.format(" CREATE (:%s %s)", "Person", "{p1:1.0}") +
                " CREATE (:Foo {foo:'foo'})-[:Rel {rel:'rel'}]->(:Bar {bar:'bar'})";
    }

    @BeforeAll
    static void initialize() throws SQLException {
        final String endpoint = String.format("bolt://%s:%d", HOSTNAME, 8182);
        PROPERTIES.put(SSH_USER, "ec2-user");
        PROPERTIES.put(SSH_HOSTNAME, "ec2-publicIP");
        PROPERTIES.put(SSH_PRIVATE_KEY_FILE, "~/path/to/pem-file.pem");
        PROPERTIES.put(SSH_STRICT_HOST_KEY_CHECKING, "false");
        PROPERTIES.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reverse default to None
        PROPERTIES.putIfAbsent(OpenCypherConnectionProperties.ENDPOINT_KEY, endpoint);
        connection = new OpenCypherConnection(new OpenCypherConnectionProperties(PROPERTIES));
        final java.sql.Statement statement = connection.createStatement();
        statement.execute(CREATE_NODES);
        databaseMetaData = connection.getMetaData();
    }

    @Disabled
    @Test
    void testGetColumns() throws SQLException {
        Assert.assertTrue(connection.isValid(1));
        final java.sql.ResultSet resultSet = databaseMetaData.getColumns(null, null, null, null);
        Assertions.assertTrue(resultSet.next());
        do {
            for (final String columnName : OpenCypherGetColumnUtilities.COLUMN_NAMES) {
                System.out.println(columnName + " - " + resultSet.getString(columnName));
            }

        } while (resultSet.next());
    }

    @Disabled
    @Test
    void testGetColumnsHuman() throws SQLException {
        final java.sql.ResultSet resultSet = databaseMetaData.getColumns(null, null, "Human", null);
        Assertions.assertTrue(resultSet.next());
        do {
            for (final String columnName : OpenCypherGetColumnUtilities.COLUMN_NAMES) {
                System.out.println(columnName + " - " + resultSet.getString(columnName));
            }

        } while (resultSet.next());
    }

    @Disabled
    @Test
    void testGetColumnsHumanDeveloper() throws SQLException {
        final java.sql.ResultSet resultSet = databaseMetaData.getColumns(null, null, "Human:Developer", null);
        Assertions.assertFalse(resultSet.next());
    }

    @Disabled
    @Test
    void testIsValid() throws SQLException {
        Assertions.assertTrue(connection.isValid(1));
    }
}
