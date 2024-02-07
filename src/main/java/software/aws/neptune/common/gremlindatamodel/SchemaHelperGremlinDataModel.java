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

package software.aws.neptune.common.gremlindatamodel;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.common.IAMHelper;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.neptune.gremlin.GremlinQueryExecutor;
import software.aws.neptune.gremlin.adapter.converter.schema.SqlSchemaGrabber;
import software.aws.neptune.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import java.sql.SQLException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class SchemaHelperGremlinDataModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaHelperGremlinDataModel.class);
    private static final int MIN_CONNECTION_POOL_SIZE = 32;
    private static final int MAX_CONNECTION_POOL_SIZE = 96;
    private static final int CONNECTION_TIMEOUT = 180 * 1000;

    private static Client getClient(final String endpoint, final int port, final boolean useIam, final boolean useSsl) {
        final Cluster.Builder builder = Cluster.build();
        builder.addContactPoint(endpoint);
        builder.port(port);
        builder.enableSsl(useSsl);
        builder.maxWaitForConnection(CONNECTION_TIMEOUT);
        builder.maxConnectionPoolSize(MAX_CONNECTION_POOL_SIZE);
        builder.minConnectionPoolSize(MIN_CONNECTION_POOL_SIZE);
        if (useIam) {
            IAMHelper.addHandshakeInterceptor(builder);
        }
        final Cluster cluster = builder.create();
        final Client client = cluster.connect();
        client.init();
        return client;
    }

    private static String getAdjustedEndpoint(final String endpoint, final MetadataCache.PathType pathType)
            throws SQLException {
        if (pathType == MetadataCache.PathType.Bolt) {
            final String[] endpointSplit = endpoint.split(":");
            if ((endpointSplit.length != 3) || (!endpointSplit[1].startsWith("//"))) {
                throw SqlError
                        .createSQLException(LOGGER, SqlState.CONNECTION_FAILURE, SqlError.INVALID_ENDPOINT, endpoint);
            }
            return endpointSplit[1].substring(2);
        } else {
            return endpoint;
        }
    }

    /**
     * Function to get the schema of the graph.
     *
     * @param endpoint Endpoint of database.
     * @param port     Port of database.
     * @param useIAM   Boolean for whether or not to use IAM.
     * @param useSsl   Boolean for whether or not to use SSL.
     * @param pathType Type of path.
     * @param scanType Scan type.
     * @return Graph Schema.
     * @throws SQLException If graph schema cannot be obtained.
     */
    public static GremlinSchema getGraphSchema(final String endpoint, final int port, final boolean useIAM,
                                               final boolean useSsl,
                                               final MetadataCache.PathType pathType,
                                               final SqlSchemaGrabber.ScanType scanType)
            throws SQLException {
        final String adjustedEndpoint = getAdjustedEndpoint(endpoint, pathType);
        return SqlSchemaGrabber.getSchema(
                traversal().withRemote(DriverRemoteConnection.using(getClient(adjustedEndpoint, port, useIAM, useSsl))),
                scanType);
    }

    /**
     * Function to get the schema of the graph through gremlin connection
     *
     * @param gremlinConnectionProperties Connection parameters.
     * @return Graph Schema.
     * @throws SQLException If graph schema cannot be obtained.s
     */
    public static GremlinSchema getGremlinGraphSchema(final GremlinConnectionProperties gremlinConnectionProperties)
            throws SQLException {
        return SqlSchemaGrabber.getSchema(
                traversal().withRemote(DriverRemoteConnection.using(GremlinQueryExecutor.getClient(gremlinConnectionProperties))),
                gremlinConnectionProperties.getScanType());
    }
}
