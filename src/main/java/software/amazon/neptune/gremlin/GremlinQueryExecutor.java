/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 */

package software.amazon.neptune.gremlin;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.QueryExecutor;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Implementation of QueryExecutor for Gremlin.
 */
public class GremlinQueryExecutor extends QueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinQueryExecutor.class);
    private static final Object CLUSTER_LOCK = new Object();
    private static Cluster cluster = null;
    private final Object completableFutureLock = new Object();
    private CompletableFuture<org.apache.tinkerpop.gremlin.driver.ResultSet> completableFuture;
    private final GremlinConnectionProperties gremlinConnectionProperties;
    private static GremlinConnectionProperties previousGremlinConnectionProperties = null;

    GremlinQueryExecutor(final GremlinConnectionProperties gremlinConnectionProperties) {
        this.gremlinConnectionProperties = gremlinConnectionProperties;
    }

    private static Cluster createCluster(final GremlinConnectionProperties properties) {
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
        if (properties.containsKey(GremlinConnectionProperties.CHANNELIZER_KEY)) {
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

        return builder.create();
    }

    private static Cluster getCluster(final GremlinConnectionProperties gremlinConnectionProperties,
                                      final boolean returnNew) {
        if (returnNew) {
            return createCluster(gremlinConnectionProperties);
        }
        if (cluster == null ||
                !propertiesEqual(previousGremlinConnectionProperties, gremlinConnectionProperties)) {
            previousGremlinConnectionProperties = gremlinConnectionProperties;
            return createCluster(gremlinConnectionProperties);
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

    private static Client getClient(final GremlinConnectionProperties gremlinConnectionProperties) {
        return getClient(gremlinConnectionProperties, null);
    }

    private static Client getClient(final GremlinConnectionProperties gremlinConnectionProperties,
                                     final String sessionId) {
        synchronized (CLUSTER_LOCK) {
            cluster = getCluster(gremlinConnectionProperties, false);
        }

        final Client client;
        if (sessionId != null) {
            client = cluster.connect(sessionId);
        } else {
            client = cluster.connect();
        }
        return client.init();
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
    public boolean isValid(final int timeout) {
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
        return null;
    }

    @Override
    public ResultSet executeGetTables(final Statement statement, final String tableName) throws SQLException {
        return null;
    }

    @Override
    public ResultSet executeGetSchemas(final Statement statement) throws SQLException {
        return null;
    }

    @Override
    public ResultSet executeGetCatalogs(final Statement statement) throws SQLException {
        return null;
    }

    @Override
    public ResultSet executeGetTableTypes(final Statement statement) throws SQLException {
        return null;
    }

    @Override
    public ResultSet executeGetColumns(final Statement statement, final String nodes) throws SQLException {
        return null;
    }

    @Override
    protected <T> T runQuery(final String query) throws SQLException {
        final Client client = getClient(gremlinConnectionProperties);

        synchronized (completableFutureLock) {
             completableFuture = client.submitAsync(query);
        }

        synchronized (completableFutureLock) {
            if (completableFuture.isCancelled()) {
                completableFuture = null;
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_CANCELED);
            }
        }

        final Iterator<Result> resultIterator;
        try {
            resultIterator = completableFuture.get().stream().iterator();
        } catch (final InterruptedException | ExecutionException e) {
            throw new SQLException(e.getMessage());
        }

        while (resultIterator.hasNext()) {
            final Result result = resultIterator.next();
        }

        synchronized (completableFutureLock) {
            completableFuture = null;
        }

        client.close();

        // TODO - return result
        return null;
    }

    @Override
    protected void performCancel() throws SQLException {
        synchronized (completableFutureLock) {
            if (completableFuture != null && !completableFuture.isDone()) {
                completableFuture.cancel(true);
            }
        }
    }
}
