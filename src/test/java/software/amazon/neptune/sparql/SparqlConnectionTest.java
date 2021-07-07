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

package software.amazon.neptune.sparql;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.sparql.mock.SparqlMockServer;
import java.sql.SQLException;
import java.util.Properties;

public class SparqlConnectionTest {
    private static final String HOSTNAME = "http://localhost";
    private static final String DATASET = "mock";
    private static final String QUERY_ENDPOINT = "query";
    private static final int PORT = SparqlMockServer.port(); // Mock server dynamically generates port
    private java.sql.Connection connection;

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
     * Function to start the mock server before testing.
     */
    @BeforeAll
    public static void initializeMockServer() {
        SparqlMockServer.ctlBeforeClass();
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
    }

    @AfterEach
    void shutdown() throws SQLException {
        connection.close();
    }

    @Test
    void testIsValid() throws SQLException {
        Assertions.assertTrue(connection.isValid(1));

        final Throwable negativeTimeout = Assertions.assertThrows(SQLException.class,
                () -> connection.isValid(-1));
        Assertions.assertEquals("Timeout value must be greater than or equal to 0",
                negativeTimeout.getMessage());

        final Properties timeoutProperties = new Properties();
        timeoutProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        // setting to non-routable IP for timeout
        timeoutProperties.put(SparqlConnectionProperties.ENDPOINT_KEY, "http://10.255.255.1");
        timeoutProperties.put(SparqlConnectionProperties.PORT_KEY, 1234);
        timeoutProperties.put(SparqlConnectionProperties.DATASET_KEY, "timeout");
        timeoutProperties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, "query");

        final java.sql.Connection timeoutConnection = new SparqlConnection(
                new SparqlConnectionProperties(timeoutProperties));
        Assertions.assertFalse(timeoutConnection.isValid(2));

        final Properties invalidProperties = new Properties();
        invalidProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        invalidProperties.put(SparqlConnectionProperties.ENDPOINT_KEY, HOSTNAME);
        invalidProperties.put(SparqlConnectionProperties.PORT_KEY, 1234);
        invalidProperties.put(SparqlConnectionProperties.DATASET_KEY, "invalid");
        invalidProperties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, "query");

        final java.sql.Connection invalidConnection = new SparqlConnection(
                new SparqlConnectionProperties(invalidProperties));
        Assertions.assertFalse(invalidConnection.isValid(1));
    }

    // TODO: AN-528 proof of concept tests for mock database - modify/remove later
    @Test
    @Disabled
    void testMockConnection() {
        final RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                // Query only.
                .queryEndpoint("/query")
                .updateEndpoint("/update");

        // inserts data into the database
        final UpdateRequest update = UpdateFactory.create("PREFIX : <http://example/> INSERT DATA { :s :p 123 }");
        // queries the database
        final Query query = QueryFactory.create("SELECT * { ?s ?p ?o } LIMIT 100");

        // connects to database, updates the database, then query it
        try (final RDFConnection conn = builder.build()) {
            System.out.println(conn.isClosed());
            conn.update(update);
            conn.queryResultSet(query, ResultSetFormatter::out);
        }
    }

    // TODO: AN-528 proof of concept tests for mock database - modify/remove later
    @Test
    @Disabled
    void testMockConnection2() {
        final String req = "" +
                "SELECT ?x " +
                "WHERE { ?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  \"John Smith\" }";

        final RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                // Query only.
                .queryEndpoint("/query")
                .updateEndpoint("/update");

        final Query query = QueryFactory.create(req);

        // Whether the connection can be reused depends on the details of the implementation.
        // See example 5.
        try (final RDFConnection conn = builder.build()) {
            conn.queryResultSet(query, ResultSetFormatter::out);
        }
    }
}
