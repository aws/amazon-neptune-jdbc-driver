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

package software.aws.neptune.sparql;

import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.aws.jdbc.utilities.AuthScheme;
import software.aws.jdbc.utilities.ConnectionProperties;
import software.aws.neptune.sparql.mock.SparqlMockServer;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

public class SparqlDatabaseMetadataTest {
    private static final String HOSTNAME = "http://localhost";
    private static final String DATASET = "mock";
    private static final String QUERY_ENDPOINT = "query";
    private static final int PORT = SparqlMockServer.port(); // Mock server dynamically generates port
    private static java.sql.Connection connection;
    private static java.sql.DatabaseMetaData databaseMetaData;

    private static Properties sparqlProperties() {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        properties.put(SparqlConnectionProperties.ENDPOINT_KEY, HOSTNAME);
        properties.put(SparqlConnectionProperties.PORT_KEY, PORT);
        properties.put(SparqlConnectionProperties.DATASET_KEY, DATASET);
        properties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, QUERY_ENDPOINT);
        return properties;
    }

    /**
     * Function to start the mock server and populate database before testing.
     */
    @BeforeAll
    public static void initializeMockServer() throws SQLException {
        SparqlMockServer.ctlBeforeClass();

        // insert into the database here
        // Query only.
        final RDFConnectionRemoteBuilder rdfConnBuilder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                // Query only.
                .queryEndpoint("/query");

        // load dataset in
        try (final RDFConnection conn = rdfConnBuilder.build()) {
            conn.load("src/test/java/software/aws/neptune/sparql/mock/sparql_mock_data.rdf");
        }
    }

    /**
     * Function to tear down server after testing.
     */
    @AfterAll
    public static void shutdownMockServer() {
        SparqlMockServer.ctlAfterClass();
    }

    @BeforeEach
    void initialize() throws SQLException {
        connection = new SparqlConnection(new SparqlConnectionProperties(sparqlProperties()));
        databaseMetaData = connection.getMetaData();
    }

    @AfterEach
    void shutdown() throws SQLException {
        connection.close();
    }

    @Test
    void testGetCatalogs() throws SQLException {
        final java.sql.ResultSet resultSet = databaseMetaData.getCatalogs();
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    void testGetSchemas() throws SQLException {
        final java.sql.ResultSet resultSet = databaseMetaData.getSchemas();
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    void testGetTableTypes() throws SQLException {
        final java.sql.ResultSet resultSet = databaseMetaData.getTableTypes();
        Assertions.assertTrue(resultSet.next());
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        Assertions.assertEquals(1, columnCount);
        Assertions.assertEquals("TABLE", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }
}
