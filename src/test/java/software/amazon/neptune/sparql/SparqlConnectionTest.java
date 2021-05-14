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
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.sparql.mock.SparqlMockServer;

import java.sql.SQLException;
import java.util.Properties;

public class SparqlConnectionTest {

    /**
     * Function to start the mock server before testing.
     */
    @BeforeAll
    public static void ctlBeforeClass() {
        SparqlMockServer.ctlBeforeClass();
    }

    /**
     * Function to tear down server after testing.
     */
    @AfterAll
    public static void ctlAfterClass() {
        SparqlMockServer.ctlAfterClass();
    }

    /**
     * Function to get a start database before each test.
     */
    @BeforeEach
    public void ctlBeforeTest() {
        SparqlMockServer.ctlBeforeTest();
    }

    /**
     * Function to get a tear down database after each test.
     */
    @AfterEach
    public void ctlAfterTest() {
        SparqlMockServer.ctlAfterTest();
    }

    private static final String HOSTNAME = "http://localhost:";
    private static final String ENDPOINT = "/mock";
    private static final String QUERY_ENDPOINT = "/query";
    private static final int PORT = SparqlMockServer.port(); // Mock server dynamically generates port?.
    private static final Properties sparqlProperties() {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        properties.put(SparqlConnectionProperties.CONTACT_POINT_KEY, HOSTNAME);
        properties.put(SparqlConnectionProperties.PORT_KEY, PORT);
        properties.put(SparqlConnectionProperties.ENDPOINT_KEY, ENDPOINT);
        properties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, QUERY_ENDPOINT);
        return properties;
    }
    private java.sql.Connection connection;

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

        final Properties invalidProperties = new Properties();
        invalidProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        invalidProperties.put(SparqlConnectionProperties.CONTACT_POINT_KEY, "invalid");
        invalidProperties.put(SparqlConnectionProperties.PORT_KEY, 1234);
        invalidProperties.put(SparqlConnectionProperties.ENDPOINT_KEY, "invalid");
        invalidProperties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, "invalid");

        final java.sql.Connection invalidConnection = new SparqlConnection(
                new SparqlConnectionProperties(invalidProperties));
        Assertions.assertFalse(invalidConnection.isValid(1));
    }

    @Test
    // TODO: proof of concept tests for mock database - modify/remove later
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
        try (RDFConnection conn = builder.build()) {
            System.out.println(conn.isClosed());
            conn.update(update);
            conn.queryResultSet(query, ResultSetFormatter::out);
        }
    }

    @Test
    // TODO: proof of concept tests for mock database - modify/remove later
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
        try (RDFConnection conn = builder.build()) {
            conn.queryResultSet(query, ResultSetFormatter::out);
        }
    }
}
