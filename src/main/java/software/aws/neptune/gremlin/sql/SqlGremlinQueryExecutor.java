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

package software.aws.neptune.gremlin.sql;

import lombok.SneakyThrows;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.SqlToGremlin;
import org.twilmes.sql.gremlin.processor.executors.SqlGremlinQueryResult;
import software.aws.jdbc.utilities.AuthScheme;
import software.aws.jdbc.utilities.SqlError;
import software.aws.jdbc.utilities.SqlState;
import software.aws.neptune.common.gremlindatamodel.GraphSchema;
import software.aws.neptune.common.gremlindatamodel.MetadataCache;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.neptune.gremlin.GremlinQueryExecutor;
import software.aws.neptune.gremlin.resultset.GremlinResultSetGetColumns;
import software.aws.neptune.gremlin.resultset.GremlinResultSetGetTables;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * Implementation of QueryExecutor for SQL via Gremlin.
 */
public class SqlGremlinQueryExecutor extends GremlinQueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlGremlinQueryExecutor.class);
    private static final Object TRAVERSAL_LOCK = new Object();
    private static SqlToGremlin sqlToGremlin = null;
    private static GraphTraversalSource graphTraversalSource = null;
    private final GremlinConnectionProperties gremlinConnectionProperties;

    /**
     * Constructor for SqlGremlinQueryExecutor.
     *
     * @param gremlinConnectionProperties GremlinConnectionProperties for connection.
     */
    public SqlGremlinQueryExecutor(final GremlinConnectionProperties gremlinConnectionProperties) throws SQLException {
        super(gremlinConnectionProperties);
        this.gremlinConnectionProperties = gremlinConnectionProperties;
        if (gremlinConnectionProperties.getAuthScheme().equals(AuthScheme.IAMSigV4)
                && (System.getenv().get("SERVICE_REGION") == null)) {
            throw new SQLException(
                    "SERVICE_REGION environment variable must be set for IAMSigV4 authentication.");
        }
    }

    /**
     * Function to release the SqlGremlinQueryExecutor resources.
     */
    public static void close() {
        try {
            synchronized (TRAVERSAL_LOCK) {
                if (graphTraversalSource != null) {
                    graphTraversalSource.close();
                }
                graphTraversalSource = null;
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to close traversal source", e);
        }
        GremlinQueryExecutor.close();
    }

    private static GraphTraversalSource getGraphTraversalSource(
            final GremlinConnectionProperties gremlinConnectionProperties)
            throws SQLException {
        synchronized (TRAVERSAL_LOCK) {
            if (graphTraversalSource == null) {
                graphTraversalSource =
                        traversal().withRemote(DriverRemoteConnection.using(getClient(gremlinConnectionProperties)));
            }
        }
        return graphTraversalSource;

    }

    private static SqlToGremlin getSqlToGremlin(final GremlinConnectionProperties gremlinConnectionProperties)
            throws SQLException {
        if (!MetadataCache.isMetadataCached()) {
            MetadataCache.updateCache(gremlinConnectionProperties.getContactPoint(), null,
                    (gremlinConnectionProperties.getAuthScheme() == AuthScheme.IAMSigV4),
                    MetadataCache.PathType.Gremlin, gremlinConnectionProperties);
        }
        if (sqlToGremlin == null) {
            sqlToGremlin = new SqlToGremlin(MetadataCache.getSchemaConfig(),
                    getGraphTraversalSource(gremlinConnectionProperties));
        }
        return sqlToGremlin;
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
        LOGGER.info("Running executeGetColumns.");
        if (!MetadataCache.isMetadataCached()) {
            // TODO AN-576: Temp isValid check. Find a better solution inside the export tool to check if connection is valid.
            if (!statement.getConnection().isValid(3000)) {
                throw new SQLException("Failed to execute getTables, could not connect to database.");
            }
            MetadataCache.updateCache(gremlinConnectionProperties.getContactPoint(), null,
                    (gremlinConnectionProperties.getAuthScheme() == AuthScheme.IAMSigV4),
                    MetadataCache.PathType.Gremlin, gremlinConnectionProperties);
        }

        final List<GraphSchema> graphSchemaList =
                MetadataCache.getFilteredCacheNodeColumnInfos(nodes);
        return new GremlinResultSetGetColumns(statement, graphSchemaList,
                MetadataCache.getFilteredResultSetInfoWithoutRowsForColumns(nodes));
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
        LOGGER.info("Running executeGetTables.");
        // TODO: Update this caching mechanism, should try to make this automatic or something.
        if (!MetadataCache.isMetadataCached()) {
            // TODO AN-576: Temp isValid check. Find a better solution inside the export tool to check if connection is valid.
            if (!statement.getConnection().isValid(3000)) {
                throw new SQLException("Failed to execute getTables, could not connect to database.");
            }
            MetadataCache.updateCache(gremlinConnectionProperties.getContactPoint(), null,
                    (gremlinConnectionProperties.getAuthScheme() == AuthScheme.IAMSigV4),
                    MetadataCache.PathType.Gremlin, gremlinConnectionProperties);
        }

        final List<GraphSchema> graphSchemaList =
                MetadataCache.getFilteredCacheNodeColumnInfos(tableName);
        return new GremlinResultSetGetTables(statement, graphSchemaList,
                MetadataCache.getFilteredResultSetInfoWithoutRowsForTables(tableName));
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
    public ResultSet executeQuery(final String sql, final Statement statement) throws SQLException {
        final Constructor<?> constructor;
        try {
            constructor = SqlGremlinResultSet.class
                    .getConstructor(java.sql.Statement.class, SqlGremlinQueryResult.class);
        } catch (final NoSuchMethodException e) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.QUERY_FAILED, e);
        }
        return runCancellableQuery(constructor, statement, sql);
    }

    @SneakyThrows
    @Override
    @SuppressWarnings("unchecked")
    protected <T> T runQuery(final String query) {
        // TODO: AN-618 Fix this backtick conversion.
        final String backtickQuery = query.replaceAll("\"", "`");
        return (T) getSqlToGremlin(gremlinConnectionProperties).execute(backtickQuery);
    }

    // TODO AN-540: Look into query cancellation.
    @Override
    protected void performCancel() {
    }
}
