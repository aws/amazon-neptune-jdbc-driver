package software.amazon.neptune.gremlin;

import org.junit.Before;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.gremlin.mock.MockGremlinDatabase;
import software.amazon.neptune.opencypher.OpenCypherConnection;
import software.amazon.neptune.opencypher.OpenCypherConnectionProperties;
import software.amazon.neptune.opencypher.mock.MockOpenCypherDatabase;
import software.amazon.neptune.opencypher.resultset.OpenCypherResultSet;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import static software.amazon.jdbc.utilities.ConnectionProperties.APPLICATION_NAME_KEY;

public class GremlinConnectionTest {
    private static final String HOSTNAME = "localhost";
    private static final String QUERY = "1+1";
    private static final Properties PROPERTIES = new Properties();
    private static final String TEST_PROP_KEY_UNSUPPORTED = "unsupported";
    private static final String TEST_PROP_VAL_UNSUPPORTED = "unsupported";
    private static final String TEST_PROP_KEY = "ConnectionTimeout";
    private static final String TEST_PROP_VAL = "1";
    private static final Properties TEST_PROP = new Properties();
    private static final Properties TEST_PROP_INITIAL = new Properties();
    private static final Properties TEST_PROP_MODIFIED = new Properties();
    private static MockOpenCypherDatabase database;
    private java.sql.Connection connection;

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() {
        MockGremlinDatabase.startGraph();
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        MockGremlinDatabase.stopGraph();
    }

    @BeforeEach
    void initialize() throws SQLException {
        PROPERTIES.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        connection = new GremlinConnection(new GremlinConnectionProperties(PROPERTIES));

        TEST_PROP.put(TEST_PROP_KEY, TEST_PROP_VAL);
        TEST_PROP_INITIAL.put(APPLICATION_NAME_KEY, Driver.APPLICATION_NAME);
        TEST_PROP_INITIAL.putAll(ConnectionProperties.DEFAULT_PROPERTIES_MAP);
        TEST_PROP_INITIAL.putAll(GremlinConnectionProperties.DEFAULT_PROPERTIES_MAP);
        TEST_PROP_INITIAL.putAll(PROPERTIES);
        TEST_PROP_MODIFIED.putAll(TEST_PROP_INITIAL);
        TEST_PROP_MODIFIED.remove(TEST_PROP_KEY);
    }

    @Test
    void testGremlinPrepareStatementType() {
        final AtomicReference<PreparedStatement> statement = new AtomicReference<>();
        Assertions.assertDoesNotThrow(() -> statement.set(connection.prepareStatement(QUERY)));
        Assertions.assertTrue(statement.get() instanceof software.amazon.jdbc.PreparedStatement);

        final AtomicReference<ResultSet> openCypherResultSet = new AtomicReference<>();
        Assertions.assertDoesNotThrow(() -> openCypherResultSet.set(statement.get().executeQuery()));
    }
}
