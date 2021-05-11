package software.amazon.neptune.sparql;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.QueryExecutor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SparqlQueryExecutor extends QueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlQueryExecutor.class);
    private static final Object DRIVER_LOCK = new Object();
    private static SparqlConnectionProperties previousSparqlConnectionProperties = null;
    private static Driver driver = null;
    private final SparqlConnectionProperties sparqlConnectionProperties;
    private final Object sessionLock = new Object();
    private Session session = null;

    SparqlQueryExecutor(final SparqlConnectionProperties sparqlConnectionProperties) {
        this.sparqlConnectionProperties = sparqlConnectionProperties;
    }

    @Override
    public int getMaxFetchSize() {
        return 0;
    }

    /**
     * Verify that connection to database is functional.
     *
     * @param timeout Time in seconds to wait for the database operation used to validate the connection to complete.
     * @return true if the connection is valid, otherwise false.
     */
    @Override
    public boolean isValid(final int timeout) {
        return false;
    }

    /**
     * Function to execute query.
     *
     * @param sparql       Query to execute.
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public ResultSet executeQuery(final String sparql, final Statement statement) throws SQLException {
        return null;
    }

    /**
     * Function to get tables.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @param tableName String table name with colon delimits.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public ResultSet executeGetTables(final Statement statement, final String tableName) throws SQLException {
        return null;
    }

    /**
     * Function to get schema.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResulSet Object containing schemas.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public ResultSet executeGetSchemas(final Statement statement) throws SQLException {
        return null;
    }

    /**
     * Function to get catalogs.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing catalogs.
     */
    @Override
    public ResultSet executeGetCatalogs(final Statement statement) throws SQLException {
        return null;
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing table types.
     */
    @Override
    public ResultSet executeGetTableTypes(final Statement statement) throws SQLException {
        return null;
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @param nodes     String containing nodes to get schema for.
     * @return java.sql.ResultSet Object containing columns.
     */
    @Override
    public ResultSet executeGetColumns(final Statement statement, final String nodes) throws SQLException {
        return null;
    }

    @Override
    protected <T> T runQuery(final String query) throws SQLException {
        return null;
    }

    @Override
    protected void performCancel() throws SQLException {

    }
}
