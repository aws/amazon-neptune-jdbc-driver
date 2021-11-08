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

package software.aws.neptune.common.gremlindatamodel;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.schema.SqlSchemaGrabber;
import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinEdgeTable;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinVertexTable;
import software.aws.neptune.common.ResultSetInfoWithoutRows;
import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetColumns;
import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetTables;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.opencypher.OpenCypherConnectionProperties;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MetadataCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataCache.class);
    private static final Object LOCK = new Object();
    @Getter
    private static GremlinSchema gremlinSchema = null;

    /**
     * Function to update the cache of the metadata.
     *
     * @param endpoint Endpoint of target database.
     * @param port     Port of target database.
     * @param useIam   Flag to use IAM or not.
     * @param useSsl   Flag to use SSL.
     * @param pathType Path type.
     * @throws SQLException Thrown if error occurs during update.
     */
    public static void updateCache(final String endpoint, final int port, final boolean useIam, final boolean useSsl,
                                   final PathType pathType, final SqlSchemaGrabber.ScanType scanType)
            throws SQLException {
        synchronized (LOCK) {
            if (gremlinSchema == null) {
                gremlinSchema =
                        SchemaHelperGremlinDataModel.getGraphSchema(endpoint, port, useIam, useSsl, pathType, scanType);
            }
        }
    }

    /**
     * Function to update the cache of the metadata.
     *
     * @param gremlinConnectionProperties GremlinConnectionProperties to use.
     * @throws SQLException Thrown if error occurs during update.
     */
    public static void updateCacheIfNotUpdated(final GremlinConnectionProperties gremlinConnectionProperties)
            throws SQLException {
        if (!isMetadataCached()) {
            updateCache(gremlinConnectionProperties.getContactPoint(), gremlinConnectionProperties.getPort(),
                    (gremlinConnectionProperties.getAuthScheme() == AuthScheme.IAMSigV4),
                    gremlinConnectionProperties.getEnableSsl(),
                    MetadataCache.PathType.Gremlin, gremlinConnectionProperties.getScanType());
        }
    }

    /**
     * Function to update the cache of the metadata.
     *
     * @param openCypherConnectionProperties OpenCypherConnectionProperties to use.
     * @throws SQLException Thrown if error occurs during update.
     */
    public static void updateCacheIfNotUpdated(final OpenCypherConnectionProperties openCypherConnectionProperties)
            throws SQLException {
        if (!isMetadataCached()) {
            updateCache(openCypherConnectionProperties.getEndpoint(), openCypherConnectionProperties.getPort(),
                    (openCypherConnectionProperties.getAuthScheme() == AuthScheme.IAMSigV4),
                    openCypherConnectionProperties.getUseEncryption(),
                    PathType.Bolt, openCypherConnectionProperties.getScanType());
        }
    }

    /**
     * Function to return whether cache is valid.
     *
     * @return True if cache is valid, false otherwise.
     */
    public static boolean isMetadataCached() {
        synchronized (LOCK) {
            return (gremlinSchema != null);
        }
    }

    /**
     * Function to filter cached NodeColumnInfo.
     *
     * @param nodeFilter Filter to apply.
     * @return Filtered NodeColumnInfo List.
     */
    public static GremlinSchema getFilteredCacheNodeColumnInfos(final String nodeFilter) throws SQLException {
        synchronized (LOCK) {
            if (gremlinSchema == null) {
                throw new SQLException("Error, cache must be updated before filtered cache can be retrieved.");
            } else if (nodeFilter == null || "%".equals(nodeFilter)) {
                return gremlinSchema;
            }
            LOGGER.info("Getting vertices.");
            final List<GremlinVertexTable> vertices = gremlinSchema.getVertices();
            LOGGER.info("Getting edges.");
            final List<GremlinEdgeTable> edges = gremlinSchema.getEdges();
            final List<GremlinVertexTable> filteredGremlinVertexTables = vertices.stream().filter(
                    table -> Arrays.stream(nodeFilter.split(":")).allMatch(f -> table.getLabel().contains(f)))
                    .collect(Collectors.toList());
            final List<GremlinEdgeTable> filteredGremlinEdgeTables = edges.stream().filter(
                    table -> Arrays.stream(nodeFilter.split(":")).allMatch(f -> table.getLabel().contains(f)))
                    .collect(Collectors.toList());
            return new GremlinSchema(filteredGremlinVertexTables, filteredGremlinEdgeTables);
        }
    }

    /**
     * Function to filter ResultSetInfoWithoutRows.
     *
     * @param nodeFilter Filter to apply.
     * @return Filtered ResultSetInfoWithoutRows Object.
     */
    public static ResultSetInfoWithoutRows getFilteredResultSetInfoWithoutRowsForColumns(
            final String nodeFilter) throws SQLException {
        return new ResultSetInfoWithoutRows(getFilteredCacheNodeColumnInfos(nodeFilter).getAllTables().stream()
                .mapToInt(table -> table.getColumns().size()).sum(), ResultSetGetColumns.getColumns());
    }

    /**
     * Function to filter ResultSetInfoWithoutRows.
     *
     * @param nodeFilter Filter to apply.
     * @return Filtered ResultSetInfoWithoutRows Object.
     */
    public static ResultSetInfoWithoutRows getFilteredResultSetInfoWithoutRowsForTables(
            final String nodeFilter) throws SQLException {
        return new ResultSetInfoWithoutRows(
                getFilteredCacheNodeColumnInfos(nodeFilter).getAllTables().size(), ResultSetGetTables.getColumns());
    }

    public enum PathType {
        Bolt,
        Gremlin
    }
}
