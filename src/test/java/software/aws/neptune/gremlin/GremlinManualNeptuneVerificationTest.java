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

package software.aws.neptune.gremlin;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import software.aws.neptune.opencypher.utilities.OpenCypherGetColumnUtilities;
import java.sql.SQLException;
import java.util.Properties;

import static software.aws.neptune.gremlin.GremlinConnectionProperties.CONTACT_POINT_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.ENABLE_SSL_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.PORT_KEY;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_HOSTNAME;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_PRIVATE_KEY_FILE;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_STRICT_HOST_KEY_CHECKING;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_USER;

@Disabled
public class GremlinManualNeptuneVerificationTest {
    private static final String ENDPOINT = "database-1.cluster-cdffsmv2nzf7.us-east-2.neptune.amazonaws.com";
    private static final String SAMPLE_QUERY = "g.V().count()";
    private static final int PORT = 8182;
    private static final String CREATE_NODES;
    private static java.sql.Connection connection;
    private static java.sql.DatabaseMetaData databaseMetaData;

    static {
        CREATE_NODES =
                "g.addV('book').property('name', 'The French Chef Cookbook').property('year' , 1968).property('ISBN', '0-394-40135-2')";
    }

    @BeforeAll
    static void initialize() throws SQLException {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set default to IAMSigV4
        properties.put(CONTACT_POINT_KEY, ENDPOINT);
        properties.put(PORT_KEY, PORT);
        properties.put(ENABLE_SSL_KEY, true);
        properties.put(SSH_USER, "ec2-user");
        properties.put(SSH_HOSTNAME, "52.14.185.245");
        properties.put(SSH_PRIVATE_KEY_FILE, "~/Downloads/EC2/neptune-test.pem");
        properties.put(SSH_STRICT_HOST_KEY_CHECKING, "false");
        connection = new GremlinConnection(new GremlinConnectionProperties(properties));
        databaseMetaData = connection.getMetaData();
        // connection.createStatement().executeQuery(CREATE_NODES);
    }

    @Disabled
    @Test
    void testGetColumns() throws SQLException {
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
    void testGetTables() throws SQLException {
        final java.sql.ResultSet resultSet = databaseMetaData.getTables(null, null, null, null);
        Assertions.assertTrue(resultSet.next());
        do {

            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                System.out.println(resultSet.getMetaData().getColumnName(i) + " - " + resultSet.getString(i));
            }
        } while (resultSet.next());
    }

    @Disabled
    @Test
    void testGetColumnsBook() throws SQLException {
        final java.sql.ResultSet resultSet = databaseMetaData.getColumns(null, null, "book", null);
        Assertions.assertTrue(resultSet.next());
        do {
            for (final String columnName : OpenCypherGetColumnUtilities.COLUMN_NAMES) {
                System.out.println(columnName + " - " + resultSet.getString(columnName));
            }

        } while (resultSet.next());
    }

    @Disabled
    @Test
    void testGremlinDB() throws SQLException {
        connection.createStatement().executeQuery("g.V().hasLabel(\"book\").valueMap()");
    }
}
