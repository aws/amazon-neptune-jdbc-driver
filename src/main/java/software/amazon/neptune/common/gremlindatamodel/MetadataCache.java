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

package software.amazon.neptune.common.gremlindatamodel;

import lombok.Getter;
import software.amazon.neptune.common.ResultSetInfoWithoutRows;
import software.amazon.neptune.common.gremlindatamodel.resultset.ResultSetGetColumns;
import software.amazon.neptune.common.gremlindatamodel.resultset.ResultSetGetTables;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MetadataCache {
    private static final Object LOCK = new Object();
    @Getter
    private static List<GraphSchema> nodeSchemaList = null;
    @Getter
    private static List<GraphSchema> edgeSchemaList = null;

    /**
     * Function to update the cache of the metadata.
     *
     * @param endpoint Endpoint of target database.
     * @param nodes    Node list to use if any.
     * @param useIAM   Flag to use IAM or not.
     * @throws SQLException Thrown if error occurs during update.
     */
    public static void updateCache(final String endpoint,
                                   final String nodes,
                                   final boolean useIAM,
                                   final PathType pathType) throws SQLException {
        synchronized (LOCK) {
            try {
                nodeSchemaList = new ArrayList<>();
                edgeSchemaList = new ArrayList<>();
                SchemaHelperGremlinDataModel
                        .getGraphSchema(endpoint, nodes, useIAM, pathType, nodeSchemaList, edgeSchemaList);
            } catch (final IOException e) {
                nodeSchemaList = null;
                edgeSchemaList = null;
                throw new SQLException(e.getMessage());
            }
        }
    }

    /**
     * Function to return whether cache is valid.
     *
     * @return True if cache is valid, false otherwise.
     */
    public static boolean isMetadataCached() {
        synchronized (LOCK) {
            return ((nodeSchemaList != null) && (edgeSchemaList != null));
        }
    }

    /**
     * Function to filter cached NodeColumnInfo.
     *
     * @param nodeFilter Filter to apply.
     * @return Filtered NodeColumnInfo List.
     */
    public static List<GraphSchema> getFilteredCacheNodeColumnInfos(
            final String nodeFilter) {
        synchronized (LOCK) {
            final List<GraphSchema> graphSchemas = new ArrayList<>();
            for (final GraphSchema graphSchema : nodeSchemaList) {
                if (nodeFilter != null && !"%".equals(nodeFilter)) {
                    if (Arrays.stream(nodeFilter.split(":"))
                            .allMatch(node -> graphSchema.getLabels().contains(node))) {
                        graphSchemas.add(graphSchema);
                    }
                } else {
                    graphSchemas.add(graphSchema);
                }
            }
            for (final GraphSchema graphSchema : edgeSchemaList) {
                if (nodeFilter != null && !"%".equals(nodeFilter)) {
                    if (Arrays.stream(nodeFilter.split(":"))
                            .allMatch(node -> graphSchema.getLabels().contains(node))) {
                        graphSchemas.add(graphSchema);
                    }
                } else {
                    graphSchemas.add(graphSchema);
                }
            }
            return graphSchemas;
        }
    }

    /**
     * Function to filter ResultSetInfoWithoutRows.
     *
     * @param nodeFilter Filter to apply.
     * @return Filtered ResultSetInfoWithoutRows Object.
     */
    public static ResultSetInfoWithoutRows getFilteredResultSetInfoWithoutRowsForColumns(
            final String nodeFilter) {
        return new ResultSetInfoWithoutRows(
                getFilteredCacheNodeColumnInfos(nodeFilter).stream().mapToInt(node -> node.getProperties().size())
                        .sum(), ResultSetGetColumns.getColumns());
    }

    /**
     * Function to filter ResultSetInfoWithoutRows.
     *
     * @param nodeFilter Filter to apply.
     * @return Filtered ResultSetInfoWithoutRows Object.
     */
    public static ResultSetInfoWithoutRows getFilteredResultSetInfoWithoutRowsForTables(
            final String nodeFilter) {
        return new ResultSetInfoWithoutRows(
                getFilteredCacheNodeColumnInfos(nodeFilter).size(), ResultSetGetTables.getColumns());
    }

    public enum PathType {
        Bolt,
        Gremlin
    }
}
