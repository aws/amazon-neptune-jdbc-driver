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

package software.amazon.neptune.gremlin;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.opencypher.utilities.OpenCypherGetColumnUtilities;
import java.sql.SQLException;
import java.util.Properties;
import static software.amazon.neptune.gremlin.GremlinConnectionProperties.CONTACT_POINT_KEY;
import static software.amazon.neptune.gremlin.GremlinConnectionProperties.ENABLE_SSL_KEY;
import static software.amazon.neptune.gremlin.GremlinConnectionProperties.PORT_KEY;

public class GremlinManualNeptuneVerificationTest {
    private static final String ENDPOINT = "iam-auth-test-lyndon.cluster-cdubgfjknn5r.us-east-1.neptune.amazonaws.com";
    private static final String SAMPLE_QUERY = "g.V().count()";
    private static final int PORT = 8182;
    private static final String CREATE_NODES;

    static {
        CREATE_NODES =
                "g.addV('book').property('name', 'The French Chef Cookbook').property('year' , 1968).property('ISBN', '0-394-40135-2')";
    }

    private java.sql.Connection connection;
    private java.sql.DatabaseMetaData databaseMetaData;

    @BeforeEach
    void initialize() throws SQLException {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set default to None
        properties.put(CONTACT_POINT_KEY, ENDPOINT);
        properties.put(PORT_KEY, PORT);
        properties.put(ENABLE_SSL_KEY, true);
        connection = new GremlinConnection(new GremlinConnectionProperties(properties));
        databaseMetaData = connection.getMetaData();
        connection.createStatement().executeQuery(CREATE_NODES);
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
