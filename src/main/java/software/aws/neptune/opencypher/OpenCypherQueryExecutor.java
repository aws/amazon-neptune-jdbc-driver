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

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.common.gremlindatamodel.MetadataCache;
import software.aws.neptune.gremlin.resultset.GremlinResultSetGetTypeInfo;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.QueryExecutor;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import software.aws.neptune.opencypher.resultset.*;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class OpenCypherQueryExecutor extends QueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherQueryExecutor.class);
    private static final Object DRIVER_LOCK = new Object();
    private static OpenCypherConnectionProperties previousOpenCypherConnectionProperties = null;
    private static Driver driver = null;
    private final OpenCypherConnectionProperties openCypherConnectionProperties;
    private final Object sessionLock = new Object();
    private Session session = null;

    OpenCypherQueryExecutor(final OpenCypherConnectionProperties openCypherConnectionProperties) {
        this.openCypherConnectionProperties = openCypherConnectionProperties;
    }

    /**
     * Function to close down the driver.
     */
    public static void close() {
        synchronized (DRIVER_LOCK) {
            if (driver != null) {
                driver.close();
                driver = null;
            }
        }
    }

    private static Driver createDriver(final Config config,
                                       final OpenCypherConnectionProperties openCypherConnectionProperties)
            throws SQLException {
        AuthToken authToken = AuthTokens.none();
        if (openCypherConnectionProperties.getAuthScheme().equals(AuthScheme.IAMSigV4)) {
            LOGGER.info("Creating driver with IAMSigV4 authentication.");
            authToken = OpenCypherIAMRequestGenerator
                    .getSignedHeader(openCypherConnectionProperties.getEndpoint(),
                            openCypherConnectionProperties.getServiceRegion());
        }
        return GraphDatabase.driver(openCypherConnectionProperties.getEndpoint(), authToken, config);
    }

    private static Driver getDriver(final Config config,
                                    final OpenCypherConnectionProperties openCypherConnectionProperties,
                                    final boolean returnNew)
            throws SQLException {
        if (returnNew) {
            return createDriver(config, openCypherConnectionProperties);
        }
        if ((driver == null) ||
                !propertiesEqual(previousOpenCypherConnectionProperties, openCypherConnectionProperties)) {
            previousOpenCypherConnectionProperties = openCypherConnectionProperties;
            return createDriver(config, openCypherConnectionProperties);
        }
        return driver;
    }

    /**
     * Function to return max fetch size.
     *
     * @return Max fetch size (Integer max value).
     */
    @Override
    public int getMaxFetchSize() {
        return Integer.MAX_VALUE;
    }

    /**
     * Verify that connection to database is functional.
     *
     * @param timeout Time in seconds to wait for the database operation used to validate the connection to complete.
     * @return true if the connection is valid, otherwise false.
     */
    public boolean isValid(final int timeout) {
        try {
            final Config config = createConfigBuilder().withConnectionTimeout(timeout, TimeUnit.SECONDS).build();
            final Driver tempDriver;
            synchronized (DRIVER_LOCK) {
                tempDriver = getDriver(config, openCypherConnectionProperties, true);
            }
            tempDriver.verifyConnectivity();
            return true;
        } catch (final Exception e) {
            LOGGER.error("Connection to database returned an error:", e);
            return false;
        }
    }

    private Config.ConfigBuilder createConfigBuilder() {
        final Config.ConfigBuilder configBuilder = Config.builder();
        final boolean useEncryption = openCypherConnectionProperties.getUseEncryption();
        if (useEncryption) {
            LOGGER.info("Creating driver with encryption.");
            configBuilder.withEncryption();
            configBuilder.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        } else {
            LOGGER.info("Creating driver without encryption.");
            configBuilder.withoutEncryption();
        }
        configBuilder.withMaxConnectionPoolSize(openCypherConnectionProperties.getConnectionPoolSize());
        configBuilder
                .withConnectionTimeout(openCypherConnectionProperties.getConnectionTimeoutMillis(),
                        TimeUnit.MILLISECONDS);

        return configBuilder;
    }

    /**
     * Function to execute query.
     *
     * @param sql       Query to execute.
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public java.sql.ResultSet executeQuery(final String sql, final java.sql.Statement statement) throws
            SQLException {
        final Constructor<?> constructor;
        try {
            constructor = OpenCypherResultSet.class
                    .getConstructor(java.sql.Statement.class, OpenCypherResultSet.ResultSetInfoWithRows.class);
        } catch (final NoSuchMethodException e) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.INVALID_QUERY_EXPRESSION,
                    SqlError.QUERY_FAILED, e);
        }
        return runCancellableQuery(constructor, statement, sql);
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
    public java.sql.ResultSet executeGetTables(final java.sql.Statement statement, final String tableName)
            throws SQLException {
        MetadataCache.updateCacheIfNotUpdated(openCypherConnectionProperties);
        return new OpenCypherResultSetGetTables(statement, MetadataCache.getFilteredCacheNodeColumnInfos(tableName),
                MetadataCache.getFilteredResultSetInfoWithoutRowsForTables(tableName));
    }

    /**
     * Function to get schema.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing schemas.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public java.sql.ResultSet executeGetSchemas(final java.sql.Statement statement)
            throws SQLException {
        return new OpenCypherResultSetGetSchemas(statement);
    }

    /**
     * Function to get catalogs.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing catalogs.
     */
    @Override
    public java.sql.ResultSet executeGetCatalogs(final java.sql.Statement statement) {
        return new OpenCypherResultSetGetCatalogs(statement);
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing table types.
     */
    @Override
    public java.sql.ResultSet executeGetTableTypes(final java.sql.Statement statement) {
        return new OpenCypherResultSetGetTableTypes(statement);
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @param nodes     String containing nodes to get schema for.
     * @return java.sql.ResultSet Object containing columns.
     */
    @Override
    public java.sql.ResultSet executeGetColumns(final java.sql.Statement statement, final String nodes)
            throws SQLException {
        MetadataCache.updateCacheIfNotUpdated(openCypherConnectionProperties);
        return new OpenCypherResultSetGetColumns(statement, MetadataCache.getFilteredCacheNodeColumnInfos(nodes),
                MetadataCache.getFilteredResultSetInfoWithoutRowsForColumns(nodes));
    }

    /**
     * Function to get type info.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing type info.
     */
    @Override
    public java.sql.ResultSet executeGetTypeInfo(final java.sql.Statement statement)
            throws SQLException {
        return new OpenCypherResultSetGetTypeInfo(statement);
    }


    @Override
    @SuppressWarnings("unchecked")
    protected <T> T runQuery(final String query) throws SQLException {
        synchronized (sessionLock) {
            synchronized (DRIVER_LOCK) {
                driver = getDriver(createConfigBuilder().build(), openCypherConnectionProperties, false);
            }
            session = driver.session();
        }

        final Result result = session.run(query);
        final List<Record> rows = result.list();
        final List<String> columns = result.keys();
        final OpenCypherResultSet.ResultSetInfoWithRows openCypherResultSet =
                new OpenCypherResultSet.ResultSetInfoWithRows(session, result, rows, columns);
        synchronized (sessionLock) {
            session = null;
        }
        return (T) openCypherResultSet;
    }

    @Override
    protected void performCancel() throws SQLException {
        synchronized (sessionLock) {
            if (session != null) {
                //noinspection deprecation
                session.reset();
            }
        }
    }
}
