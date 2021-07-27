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

package software.aws.neptune.opencypher;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.jdbc.utilities.AuthScheme;
import software.aws.neptune.opencypher.utilities.OpenCypherGetColumnUtilities;
import java.sql.SQLException;
import java.util.Properties;

public class OpenCypherManualNeptuneVerificationTest {

    private static final String HOSTNAME = "test-jdbc-3.cluster-cdubgfjknn5r.us-east-1.neptune.amazonaws.com";
    private static final Properties PROPERTIES = new Properties();
    private static final String CREATE_NODES;

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

    private java.sql.DatabaseMetaData databaseMetaData;

    @BeforeEach
    void initialize() throws SQLException {
        final String endpoint = String.format("bolt://%s:%d", HOSTNAME, 8182);
        /*final Config config = Config.builder()
                .withConnectionTimeout(3, TimeUnit.SECONDS)
                .withMaxConnectionPoolSize(1000)
                .withEncryption()
                .withTrustStrategy(Config.TrustStrategy.trustAllCertificates())
                .build();

        final Driver driver = GraphDatabase.driver(endpoint, null, config);
        driver.verifyConnectivity();*/
        PROPERTIES.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reverse default to None
        PROPERTIES.putIfAbsent(OpenCypherConnectionProperties.ENDPOINT_KEY, endpoint);
        System.out.println("Endpoint " + endpoint);
        final java.sql.Connection connection = new OpenCypherConnection(new OpenCypherConnectionProperties(PROPERTIES));
        final java.sql.Statement statement = connection.createStatement();
        // statement.execute(CREATE_NODES);
        databaseMetaData = connection.getMetaData();
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
}
