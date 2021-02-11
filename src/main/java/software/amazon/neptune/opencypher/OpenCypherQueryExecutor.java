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

package software.amazon.neptune.opencypher;

import lombok.SneakyThrows;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import software.amazon.neptune.opencypher.resultset.OpenCypherResultSet;
import software.amazon.neptune.opencypher.resultset.OpenCypherResultSetGetCatalogs;
import software.amazon.neptune.opencypher.resultset.OpenCypherResultSetGetColumns;
import software.amazon.neptune.opencypher.resultset.OpenCypherResultSetGetSchemas;
import software.amazon.neptune.opencypher.resultset.OpenCypherResultSetGetTableTypes;
import software.amazon.neptune.opencypher.resultset.OpenCypherResultSetGetTables;
import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * OpenCypher implementation of QueryExecution.
 */
public class OpenCypherQueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherQueryExecutor.class);
    private static final int MAX_FETCH_SIZE = Integer.MAX_VALUE;
    private final OpenCypherConnectionProperties connectionProperties;
    private final Object lock = new Object();
    private final Driver driver;
    private final Config config;
    private Session session;
    private SessionConfig sessionConfig;
    private int queryTimeout = -1;
    private boolean queryExecuted = false;
    private boolean queryCancelled = false;

    /**
     * OpenCypherQueryExecutor constructor.
     *
     * @param connectionProperties properties to use for query executon.
     */
    OpenCypherQueryExecutor(final OpenCypherConnectionProperties connectionProperties) throws SQLException {
        this.connectionProperties = connectionProperties;

        // TODO: Implement authentication.
        // final String user = properties.getUser();
        // final String password = properties.getPassword();
        // AuthTokens.basic(this.user, this.password), this.config);

        // Session config properties.
        this.sessionConfig = SessionConfig.builder().build();
        // Driver config properties.
        this.config = createConfigBuilder(connectionProperties).build();
        this.driver = createDriver(connectionProperties.getEndpoint(), config, connectionProperties);
    }

    /**
     * Verify that connection to database is functional.
     *
     * @param endpoint Connection endpoint.
     * @param timeout  Time in milliseconds to wait for the database operation used to validate the connection to complete.
     * @return true if the connection is valid, otherwise false.
     */
    public static boolean isValid(final String endpoint, final int timeout,
                                  final OpenCypherConnectionProperties properties) {
        try {
            final Config config = createConfigBuilder(properties).build();
            final Driver tempDriver = createDriver(endpoint, config, properties);
            tempDriver.verifyConnectivity();
            return true;
        } catch (final Exception e) {
            LOGGER.error("Connection to database returned an error:", e);
            return false;
        }
    }

    /**
     * Creates ConfigBuilder based on the OpenCypher connection properties.
     *
     * @param properties The OpenCypher connection properties.
     * @return The ConfigBuilder based on the OpenCypher connection properties.
     */
    private static Config.ConfigBuilder createConfigBuilder(final OpenCypherConnectionProperties properties) {
        final Config.ConfigBuilder configBuilder = Config.builder();

        final boolean useEncryption = properties.getUseEncryption();
        if (useEncryption) {
            configBuilder.withEncryption();
            configBuilder.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        } else {
            configBuilder.withoutEncryption();
        }
        configBuilder.withConnectionTimeout(properties.getConnectionTimeout(), TimeUnit.MILLISECONDS);
        // .withTrustStrategy(Config.TrustStrategy.trustAllCertificates()) // Required for Neptune manual test
        // .withFetchSize(properties.getFetchSize())

        return configBuilder;
    }

    private static Driver createDriver(final String endpoint, final Config config,
                                       final OpenCypherConnectionProperties properties)
            throws SQLException {
        if (properties.getAuthScheme().equals(AuthScheme.IAMSigV4)) {
            LOGGER.info("Connection with IAMSigV4 authentication.");
            return GraphDatabase.driver(endpoint,
                    OpenCypherIAMRequestGenerator.getSignedHeader(endpoint, properties.getRegion()),
                    config);
        } else {
            LOGGER.info("Connection with no authentication.");
            return GraphDatabase.driver(endpoint, config);
        }
    }

    /**
     * This value overrides the default fetch size set in driver's config properties.
     *
     * @param fetchSize Number of records to return by query.
     */
    protected void setFetchSize(final int fetchSize) {
        this.sessionConfig = SessionConfig.builder()
                .withFetchSize(fetchSize)
                .build();
    }

    protected int getMaxFetchSize() throws SQLException {
        return MAX_FETCH_SIZE;
    }

    /**
     * Function to execute query.
     *
     * @param sql       Query to execute.
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @SneakyThrows
    public java.sql.ResultSet executeQuery(final String sql, final java.sql.Statement statement) throws
            SQLException {
        final Constructor<?> constructor =
                OpenCypherResultSet.class.getConstructor(java.sql.Statement.class, Session.class,
                        Result.class,
                        List.class, List.class);
        return runQuery(constructor, statement, sql);
    }

    /**
     * Function to get tables.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @SneakyThrows
    public java.sql.ResultSet executeGetTables(final java.sql.Statement statement, final String tableName)
            throws SQLException {
        final Constructor<?> constructor =
                OpenCypherResultSetGetTables.class.getConstructor(java.sql.Statement.class, Session.class,
                        Result.class,
                        List.class, List.class);
        final String query = tableName == null ? "MATCH (n) RETURN DISTINCT LABELS(n)" :
                String.format("MATCH (n:%s) RETURN DISTINCT LABELS(n)", tableName);
        return runQuery(constructor, statement, query);
    }

    /**
     * Function to get schema.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResulSet Object containing schemas.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @SneakyThrows
    public java.sql.ResultSet executeGetSchemas(final java.sql.Statement statement)
            throws SQLException {
        return new OpenCypherResultSetGetSchemas(statement);
    }

    /**
     * Function to get catalogs.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResulSet Object containing catalogs.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @SneakyThrows
    public java.sql.ResultSet executeGetCatalogs(final java.sql.Statement statement)
            throws SQLException {
        return new OpenCypherResultSetGetCatalogs(statement);
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResulSet Object containing table types.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @SneakyThrows
    public java.sql.ResultSet executeGetTableTypes(final java.sql.Statement statement)
            throws SQLException {
        return new OpenCypherResultSetGetTableTypes(statement);
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @param nodes     String containing nodes to get schema for.
     * @return java.sql.ResulSet Object containing columns.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @SneakyThrows
    public java.sql.ResultSet executeGetColumns(final java.sql.Statement statement, final String nodes)
            throws SQLException {
        return new OpenCypherResultSetGetColumns(statement,
                OpenCypherSchemaHelper.getGraphSchema(connectionProperties.getEndpoint(), nodes));
    }

    @SneakyThrows
    private java.sql.ResultSet runQuery(final Constructor<?> constructor, final java.sql.Statement statement,
                                        final String query) throws SQLException {
        synchronized (lock) {
            queryCancelled = false;
            queryExecuted = false;
        }

        try {
            session = driver.session(sessionConfig);
            final Result result = session.run(query);
            final List<Record> rows = result.list();
            final List<String> columns = result.keys();
            synchronized (lock) {
                if (queryCancelled) {
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_CANCELED);
                }
                queryExecuted = true;
            }
            return (java.sql.ResultSet) constructor.newInstance(
                    statement, session, result, rows, columns);

        } catch (final RuntimeException e) {
            synchronized (lock) {
                if (queryCancelled) {
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_CANCELED);
                } else {
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_FAILED, e.getMessage());
                }
            }
        }
    }

    /**
     * Function to cancel running query.
     * This has to be run in the different thread from the one running the query.
     *
     * @throws SQLException if query cancellation fails.
     */
    protected void cancelQuery() throws SQLException {
        LOGGER.info("Cancel query invoked.");
        synchronized (lock) {
            if (session == null) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_NOT_STARTED);
            }

            if (queryCancelled) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_CANCELED);
            }

            if (!queryExecuted) {
                //noinspection deprecation
                session.reset();
                LOGGER.debug("Cancel query succeeded.");
                queryCancelled = true;
            } else {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_CANNOT_BE_CANCELLED);
            }
        }
    }

    /**
     * Get query execution timeout in seconds.
     *
     * @return Query execution timeout in seconds.
     */
    public int getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Set query execution timeout to the timeout in seconds.
     *
     * @param seconds Time in seconds to set query timeout to.
     */
    public void setQueryTimeout(final int seconds) {
        queryTimeout = seconds;
    }
}
