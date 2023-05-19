/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.gremlin;

import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.common.IAMHelper;
import software.aws.neptune.common.gremlindatamodel.MetadataCache;
import software.aws.neptune.gremlin.resultset.GremlinResultSet;
import software.aws.neptune.gremlin.resultset.GremlinResultSetGetCatalogs;
import software.aws.neptune.gremlin.resultset.GremlinResultSetGetColumns;
import software.aws.neptune.gremlin.resultset.GremlinResultSetGetSchemas;
import software.aws.neptune.gremlin.resultset.GremlinResultSetGetTableTypes;
import software.aws.neptune.gremlin.resultset.GremlinResultSetGetTables;
import software.aws.neptune.gremlin.resultset.GremlinResultSetGetTypeInfo;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.QueryExecutor;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of QueryExecutor for Gremlin.
 */
public class GremlinQueryExecutor extends QueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinQueryExecutor.class);
    private static final Object CLUSTER_LOCK = new Object();
    private static Cluster cluster = null;
    private static GremlinConnectionProperties previousGremlinConnectionProperties = null;
    private final Object completableFutureLock = new Object();
    private final GremlinConnectionProperties gremlinConnectionProperties;
    private CompletableFuture<org.apache.tinkerpop.gremlin.driver.ResultSet> completableFuture;

    /**
     * GremlinQueryExecutor constructor.
     *
     * @param gremlinConnectionProperties GremlinConnectionProperties for use in the executor.
     */
    public GremlinQueryExecutor(final GremlinConnectionProperties gremlinConnectionProperties) {
        this.gremlinConnectionProperties = gremlinConnectionProperties;
    }

    /**
     * Function to create a cluster builder for Gremlin.
     *
     * @param properties Connection properties to use.
     * @return Cluster builder based on connection properties.
     * @throws SQLException if internal functions in the properties fail.
     */
    public static Cluster.Builder createClusterBuilder(final GremlinConnectionProperties properties)
            throws SQLException {
        final Cluster.Builder builder = Cluster.build();

        if (properties.containsKey(GremlinConnectionProperties.CONTACT_POINT_KEY)) {
            builder.addContactPoint(properties.getContactPoint());
        }
        if (properties.containsKey(GremlinConnectionProperties.PATH_KEY)) {
            builder.path(properties.getPath());
        }
        if (properties.containsKey(GremlinConnectionProperties.PORT_KEY)) {
            builder.port(properties.getPort());
        }
        if (properties.containsKey(GremlinConnectionProperties.SERIALIZER_KEY)) {
            if (properties.isSerializerObject()) {
                builder.serializer(properties.getSerializerObject());
            } else if (properties.isSerializerEnum()) {
                builder.serializer(properties.getSerializerEnum());
            } else if (properties.isChannelizerString()) {
                builder.serializer(properties.getSerializerString());
            }
        }
        if (properties.containsKey(GremlinConnectionProperties.ENABLE_SSL_KEY)) {
            builder.enableSsl(properties.getEnableSsl());
        }
        if (properties.containsKey(GremlinConnectionProperties.SSL_CONTEXT_KEY)) {
            builder.sslContext(properties.getSslContext());
        }
        if (properties.containsKey(GremlinConnectionProperties.SSL_ENABLED_PROTOCOLS_KEY)) {
            builder.sslEnabledProtocols(properties.getSslEnabledProtocols());
        }
        if (properties.containsKey(GremlinConnectionProperties.SSL_CIPHER_SUITES_KEY)) {
            builder.sslCipherSuites(properties.getSslCipherSuites());
        }
        if (properties.containsKey(GremlinConnectionProperties.SSL_SKIP_VALIDATION_KEY)) {
            builder.sslSkipCertValidation(properties.getSslSkipCertValidation());
        }
        if (properties.containsKey(GremlinConnectionProperties.KEY_STORE_KEY)) {
            builder.keyStore(properties.getKeyStore());
        }
        if (properties.containsKey(GremlinConnectionProperties.KEY_STORE_PASSWORD_KEY)) {
            builder.keyStorePassword(properties.getKeyStorePassword());
        }
        if (properties.containsKey(GremlinConnectionProperties.KEY_STORE_TYPE_KEY)) {
            builder.keyStoreType(properties.getKeyStoreType());
        }
        if (properties.containsKey(GremlinConnectionProperties.TRUST_STORE_KEY)) {
            builder.trustStore(properties.getTrustStore());
        }
        if (properties.containsKey(GremlinConnectionProperties.TRUST_STORE_PASSWORD_KEY)) {
            builder.trustStorePassword(properties.getTrustStorePassword());
        }
        if (properties.containsKey(GremlinConnectionProperties.TRUST_STORE_TYPE_KEY)) {
            builder.trustStoreType(properties.getTrustStoreType());
        }
        if (properties.containsKey(GremlinConnectionProperties.NIO_POOL_SIZE_KEY)) {
            builder.nioPoolSize(properties.getNioPoolSize());
        }
        if (properties.containsKey(GremlinConnectionProperties.WORKER_POOL_SIZE_KEY)) {
            builder.workerPoolSize(properties.getWorkerPoolSize());
        }
        if (properties.containsKey(GremlinConnectionProperties.MAX_CONNECTION_POOL_SIZE_KEY)) {
            builder.maxConnectionPoolSize(properties.getMaxConnectionPoolSize());
        }
        if (properties.containsKey(GremlinConnectionProperties.MIN_CONNECTION_POOL_SIZE_KEY)) {
            builder.minConnectionPoolSize(properties.getMinConnectionPoolSize());
        }
        if (properties.containsKey(GremlinConnectionProperties.MAX_IN_PROCESS_PER_CONNECTION_KEY)) {
            builder.maxInProcessPerConnection(properties.getMaxInProcessPerConnection());
        }
        if (properties.containsKey(GremlinConnectionProperties.MIN_IN_PROCESS_PER_CONNECTION_KEY)) {
            builder.minInProcessPerConnection(properties.getMinInProcessPerConnection());
        }
        if (properties.containsKey(GremlinConnectionProperties.MAX_SIMULT_USAGE_PER_CONNECTION_KEY)) {
            builder.maxSimultaneousUsagePerConnection(properties.getMaxSimultaneousUsagePerConnection());
        }
        if (properties.containsKey(GremlinConnectionProperties.MIN_SIMULT_USAGE_PER_CONNECTION_KEY)) {
            builder.minSimultaneousUsagePerConnection(properties.getMinSimultaneousUsagePerConnection());
        }
        if (properties.getAuthScheme() == AuthScheme.IAMSigV4) {
            IAMHelper.addHandshakeInterceptor(builder);
        } else if (properties.containsKey(GremlinConnectionProperties.CHANNELIZER_KEY)) {
            if (properties.isChannelizerGeneric()) {
                builder.channelizer(properties.getChannelizerGeneric());
            } else if (properties.isChannelizerString()) {
                builder.channelizer(properties.getChannelizerString());
            }
        }
        if (properties.containsKey(GremlinConnectionProperties.KEEPALIVE_INTERVAL_KEY)) {
            builder.keepAliveInterval(properties.getKeepAliveInterval());
        }
        if (properties.containsKey(GremlinConnectionProperties.RESULT_ITERATION_BATCH_SIZE_KEY)) {
            builder.resultIterationBatchSize(properties.getResultIterationBatchSize());
        }
        if (properties.containsKey(GremlinConnectionProperties.MAX_WAIT_FOR_CONNECTION_KEY)) {
            builder.maxWaitForConnection(properties.getMaxWaitForConnection());
        }
        if (properties.containsKey(GremlinConnectionProperties.MAX_WAIT_FOR_CLOSE_KEY)) {
            builder.maxWaitForClose(properties.getMaxWaitForClose());
        }
        if (properties.containsKey(GremlinConnectionProperties.MAX_CONTENT_LENGTH_KEY)) {
            builder.maxContentLength(properties.getMaxContentLength());
        }
        if (properties.containsKey(GremlinConnectionProperties.VALIDATION_REQUEST_KEY)) {
            builder.validationRequest(properties.getValidationRequest());
        }
        if (properties.containsKey(GremlinConnectionProperties.RECONNECT_INTERVAL_KEY)) {
            builder.reconnectInterval(properties.getReconnectInterval());
        }
        if (properties.containsKey(GremlinConnectionProperties.LOAD_BALANCING_STRATEGY_KEY)) {
            builder.loadBalancingStrategy(properties.getLoadBalancingStrategy());
        }

        return builder;
    }

    protected static Cluster getCluster(final GremlinConnectionProperties gremlinConnectionProperties)
            throws SQLException {
        if (cluster == null ||
                !propertiesEqual(previousGremlinConnectionProperties, gremlinConnectionProperties)) {
            previousGremlinConnectionProperties = gremlinConnectionProperties;
            return createClusterBuilder(gremlinConnectionProperties).create();
        }
        return cluster;
    }

    /**
     * Function to close down the cluster.
     */
    public static void close() {
        synchronized (CLUSTER_LOCK) {
            if (cluster != null) {
                cluster.close();
                cluster = null;
            }
        }
    }

    protected static Client getClient(final GremlinConnectionProperties gremlinConnectionProperties)
            throws SQLException {
        synchronized (CLUSTER_LOCK) {
            cluster = getCluster(gremlinConnectionProperties);
            return cluster.connect().init();
        }
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
    @Override
    @SneakyThrows
    public boolean isValid(final int timeout) {
        LOGGER.info("Checking timeout " + timeout + ".");
        final Cluster tempCluster =
                GremlinQueryExecutor.createClusterBuilder(gremlinConnectionProperties).maxWaitForConnection(timeout * 1000)
                        .create();
        final Client tempClient = tempCluster.connect();
        try {
            tempClient.init();

            // Neptune doesn't support arbitrary math queries, but the below command is valid in Gremlin and is basically
            // saying return 0.
            final CompletableFuture<List<Result>> tempCompletableFuture = tempClient.submit("g.inject(0)").all();
            tempCompletableFuture.get(timeout, TimeUnit.SECONDS);

            return true;
        } catch (final Exception e) {
            LOGGER.error("Connecting to database failed.", e);
        } finally {
            tempClient.close();
            tempCluster.close();
        }
        return false;
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
        LOGGER.info("GremlinQueryExecutor executeQuery");
        final Constructor<?> constructor;
        try {
            constructor = GremlinResultSet.class
                    .getConstructor(java.sql.Statement.class, GremlinResultSet.ResultSetInfoWithRows.class);
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
        LOGGER.info("GremlinQueryExecutor executeGetTables");
        final String endpoint = this.gremlinConnectionProperties.getContactPoint();
        if (!MetadataCache.isMetadataCached(endpoint)) {
            // TODO AN-576: Temp isValid check. Find a better solution inside the export tool to check if connection is valid.
            if (!statement.getConnection().isValid(3000)) {
                throw new SQLException("Failed to execute getTables, could not connect to database.");
            }
        }
        MetadataCache.updateCacheIfNotUpdated(gremlinConnectionProperties);
        return new GremlinResultSetGetTables(statement,
                MetadataCache.getFilteredCacheNodeColumnInfos(tableName, endpoint),
                MetadataCache.getFilteredResultSetInfoWithoutRowsForTables(tableName, endpoint));
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
        LOGGER.info("GremlinQueryExecutor executeGetSchemas");
        return new GremlinResultSetGetSchemas(statement);
    }

    /**
     * Function to get catalogs.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing catalogs.
     */
    @Override
    public java.sql.ResultSet executeGetCatalogs(final java.sql.Statement statement) {
        LOGGER.info("GremlinQueryExecutor executeGetCatalogs");
        return new GremlinResultSetGetCatalogs(statement);
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing table types.
     */
    @Override
    public java.sql.ResultSet executeGetTableTypes(final java.sql.Statement statement) {
        LOGGER.info("GremlinQueryExecutor executeGetTableTypes");
        return new GremlinResultSetGetTableTypes(statement);
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
        LOGGER.info("GremlinQueryExecutor executeGetColumns");
        final String endpoint = this.gremlinConnectionProperties.getContactPoint();
        if (!MetadataCache.isMetadataCached(endpoint)) {
            // TODO AN-576: Temp isValid check. Find a better solution inside the export tool to check if connection is valid.
            if (!statement.getConnection().isValid(3000)) {
                throw new SQLException("Failed to execute getTables, could not connect to database.");
            }
        }
        MetadataCache.updateCacheIfNotUpdated(gremlinConnectionProperties);
        return new GremlinResultSetGetColumns(statement,
                MetadataCache.getFilteredCacheNodeColumnInfos(nodes, endpoint),
                MetadataCache.getFilteredResultSetInfoWithoutRowsForColumns(nodes, endpoint));
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
        LOGGER.info("GremlinQueryExecutor executeGetTypeInfo");
        return new GremlinResultSetGetTypeInfo(statement);
    }

    @SneakyThrows
    @Override
    @SuppressWarnings("unchecked")
    protected <T> T runQuery(final String query) throws SQLException {
        final Client client = getClient(gremlinConnectionProperties);

        synchronized (completableFutureLock) {
            completableFuture = client.submitAsync(query);
        }

        final List<Result> results = completableFuture.get().all().get();
        final List<Map<String, Object>> rows = new ArrayList<>();
        final Map<String, Class<?>> columns = new HashMap<>();
        long unnamedColumnIndex = 0L;
        for (final Object result : results.stream().map(Result::getObject).collect(Collectors.toList())) {
            if (result instanceof LinkedHashMap) {
                // We don't know key or value types, so pull it out raw.
                final Map<?, ?> uncastedRow = (LinkedHashMap<?, ?>) result;

                // Convert generic key types to string and insert in new map with corresponding value.
                final Map<String, Object> row = new HashMap<>();
                uncastedRow.forEach((key, value) -> row.put(key.toString(), value));

                // Add row to List of rows.
                rows.add(row);

                // Get columns from row and put in columns List if they aren't already in there.
                for (final String key : row.keySet()) {
                    if (!columns.containsKey(key)) {
                        final Object value = row.get(key);
                        if (GremlinTypeMapping.checkContains(value.getClass())) {
                            columns.put(key, value.getClass());
                        } else {
                            columns.put(key, String.class);
                        }
                    } else if (columns.get(key) != row.get(key)) {
                        columns.put(key, String.class);
                    }
                }
            } else if (GremlinTypeMapping.checkContains(result.getClass())) {
                // Result is scalar - generate a new key for the column
                unnamedColumnIndex = findNextValidColumnIndex(columns, unnamedColumnIndex);
                final String key = generateColumnKey(unnamedColumnIndex);
                columns.put(key, result.getClass());

                // Create and add new row with generated key
                final Map<String, Object> row = new HashMap<>();
                row.put(key, result);
                rows.add(row);
            } else {
                // If not a map nor scalar best way to handle it seems to be to issue a warning.
                LOGGER.warn(String.format("Result of type '%s' is not convertible to a Map or Scalar of supported type and will be skipped.",
                        result.getClass().getCanonicalName()));
            }
        }

        final List<String> listColumns = new ArrayList<>(columns.keySet());
        return (T) new GremlinResultSet.ResultSetInfoWithRows(rows, columns, listColumns);
    }

    @Override
    protected void performCancel() throws SQLException {
        synchronized (completableFutureLock) {
            if (completableFuture != null && !completableFuture.isDone()) {
                completableFuture.cancel(true);
            }
        }
    }

    private long findNextValidColumnIndex(@NonNull final Map<String, Class<?>> columns, final long currentIndex) throws SQLException {
        long index = currentIndex;
        // While there is a conflict with an existing key increment and regenerate the column key
        while (columns.containsKey(generateColumnKey(index))) {
            if (index == Long.MAX_VALUE) {
                LOGGER.error(String.format("Reached the maximum number of column keys available for scalar columns: %d",
                        index));
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.NUMERIC_VALUE_OUT_OF_RANGE,
                        SqlError.INVALID_MAX_FIELD_SIZE);
            }
            index++;
        }
        return index;
    }

    private String generateColumnKey(@NonNull final Long unnamedColumnIndex) {
        return String.format("_col%d", unnamedColumnIndex);
    }
}
