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
 */

package software.amazon.neptune.sparql.resultset;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.sparql.SparqlConnection;
import software.amazon.neptune.sparql.SparqlConnectionProperties;
import software.amazon.neptune.sparql.mock.SparqlMockServer;
import java.sql.SQLException;
import java.util.Properties;

public class SparqlResultSetTest {
    private static final String HOSTNAME = "http://localhost";
    private static final String DATASET = "mock";
    private static final String QUERY_ENDPOINT = "query";
    private static final int PORT = SparqlMockServer.port(); // Mock server dynamically generates port
    private static java.sql.Connection connection;
    private static java.sql.ResultSet resultSet;
    private static RDFConnectionRemoteBuilder rdfConnBuilder;

    private static Properties sparqlProperties() {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        properties.put(SparqlConnectionProperties.CONTACT_POINT_KEY, HOSTNAME);
        properties.put(SparqlConnectionProperties.PORT_KEY, PORT);
        properties.put(SparqlConnectionProperties.DATASET_KEY, DATASET);
        properties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, QUERY_ENDPOINT);
        return properties;
    }

    /**
     * Function to start the mock server and populate database before testing.
     */
    @BeforeAll
    public static void ctlBeforeClass() throws SQLException {
        SparqlMockServer.ctlBeforeClass();

        // TODO: refactor this data insertion else where (e.g. mock server)?
        // insert into the database here
        rdfConnBuilder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                // Query only.
                .queryEndpoint("/query")
                .updateEndpoint("/update");

        // load dataset in
        try (final RDFConnection conn = rdfConnBuilder.build()) {
            conn.load("src/test/java/software/amazon/neptune/sparql/mock/vc-db-1.rdf");
        }
    }

    /**
     * Function to tear down server after testing.
     */
    @AfterAll
    public static void ctlAfterClass() {
        SparqlMockServer.ctlAfterClass();
    }

    // not sure of uses for this yet
    private static SparqlResultSet getVal(final String label) throws SQLException {
        final SparqlResultSet result = (SparqlResultSet) connection.createStatement()
                .executeQuery("SELECT * { ?s ?p ?o } LIMIT 100");
        Assertions.assertTrue(result.next());

        return result;
    }

    // to printout result in format of Jena ResultSet
    private static void printJenaResultSetOut(final String query) {
        final Query jenaQuery = QueryFactory.create(query);
        try (final RDFConnection conn = rdfConnBuilder.build()) {
            conn.queryResultSet(jenaQuery, ResultSetFormatter::out);
        }
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
    void testQueryResult() throws SQLException {
        final String query = "SELECT ?x ?fname WHERE {?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  ?fname}";
        final SparqlResultSet result = (SparqlResultSet) connection.createStatement()
                .executeQuery(query);
        printJenaResultSetOut(query);

        // next() increments the RowIndex everytime it is called (see ResultSet)
        // Assertions.assertTrue(result.next());
        System.out.println("ROW INDEX: " + result.getRowIndex());

        while (result.next()) {
            System.out.println("ROW INDEX: " + result.getRowIndex());
            for (int i = 1; i <= 2; i++) {
                System.out.println(result.getConvertedValue(i));
            }
        }
        System.out.println("ROW INDEX: " + result.getRowIndex());

        // would rowIndex be set to 1 after exceeding or set to be row count at the end?
        Assertions.assertFalse(result.next());
        System.out.println("ROW INDEX: " + result.getRowIndex());

    }

    @Test
    void testQueryResult2() throws SQLException {
        final String query = "PREFIX vcard:      <http://www.w3.org/2001/vcard-rdf/3.0#>\n" +
                "\n" +
                "SELECT ?y ?givenName\n" +
                "WHERE\n" +
                " { ?y vcard:Family \"Smith\" .\n" +
                "   ?y vcard:Given  ?givenName .\n" +
                " }";
        final SparqlResultSet result = (SparqlResultSet) connection.createStatement()
                .executeQuery(query);
        printJenaResultSetOut(query);

        // next() increments the RowIndex everytime it is called (see ResultSet)
        // Assertions.assertTrue(result.next());

        while (result.next()) {
            for (int i = 1; i <= 2; i++) {
                System.out.println(result.getConvertedValue(i));
            }
        }
    }

    @Test
    void testMockConnection() {
        // TODO: taken from SparqlConnectionTest, to be deleted
        final RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                // Query only.
                .queryEndpoint("/query")
                .updateEndpoint("/update");

        // queries the database
        final Query query = QueryFactory.create("SELECT * { ?s ?p ?o } LIMIT 100");

        // connects to database, updates the database, then query it
        try (final RDFConnection conn = builder.build()) {
            conn.queryResultSet(query, ResultSetFormatter::out);
        }
    }
}
