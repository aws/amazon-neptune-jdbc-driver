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

package software.amazon.neptune.opencypher.resultset;

import lombok.Getter;
import software.amazon.neptune.opencypher.OpenCypherSchemaHelper;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OpenCypherMetadataCache {
    @Getter
    private static List<OpenCypherResultSetGetColumns.NodeColumnInfo> cachedNodeColumnInfos = null;

    /**
     * Function to update the cache of the OpenCypherMetadata
     *
     * @param endpoint Endpoint of target database.
     * @param nodes    Node list to use if any.
     * @param useIAM   Flag to use IAM or not.
     * @throws SQLException Thrown if error occurs during update.
     */
    public static void updateCache(final String endpoint,
                                   final String nodes,
                                   final boolean useIAM) throws SQLException {
        cachedNodeColumnInfos = OpenCypherSchemaHelper.getGraphSchema(endpoint, nodes, useIAM);
    }

    /**
     * Function to return whether cache is valid.
     *
     * @return True if cache is valid, false otherwise.
     */
    public static boolean isOpenCypherMetadataCached() {
        return cachedNodeColumnInfos != null;
    }

    /**
     * Function to filter cached NodeColumnInfo.
     *
     * @param nodeFilter Filter to apply.
     * @return Filtered NodeColumnInfo List.
     */
    public static List<OpenCypherResultSetGetColumns.NodeColumnInfo> getFilteredCacheNodeColumnInfos(
            final String nodeFilter) {
        final List<OpenCypherResultSetGetColumns.NodeColumnInfo> nodeColumnInfos = new ArrayList<>();
        for (final OpenCypherResultSetGetColumns.NodeColumnInfo nodeColumnInfo : cachedNodeColumnInfos) {
            if (nodeFilter != null) {
                if (Arrays.stream(nodeFilter.split(":")).allMatch(node -> nodeColumnInfo.getLabels().contains(node))) {
                    nodeColumnInfos.add(nodeColumnInfo);
                }
            } else {
                nodeColumnInfos.add(nodeColumnInfo);
            }
        }
        return nodeColumnInfos;
    }

    /**
     * Function to filter ResultSetInfoWithoutRows.
     *
     * @param nodeFilter Filter to apply.
     * @return Filtered ResultSetInfoWithoutRows Object.
     */
    public static OpenCypherResultSet.ResultSetInfoWithoutRows getFilteredResultSetInfoWithoutRowsForColumns(
            final String nodeFilter) {
        return new OpenCypherResultSet.ResultSetInfoWithoutRows(null, null,
                getFilteredCacheNodeColumnInfos(nodeFilter).stream().mapToInt(node -> node.getProperties().size())
                        .sum(), OpenCypherResultSetGetColumns.getColumns());
    }

    /**
     * Function to filter ResultSetInfoWithoutRows.
     *
     * @param nodeFilter Filter to apply.
     * @return Filtered ResultSetInfoWithoutRows Object.
     */
    public static OpenCypherResultSet.ResultSetInfoWithoutRows getFilteredResultSetInfoWithoutRowsForTables(
            final String nodeFilter) {
        return new OpenCypherResultSet.ResultSetInfoWithoutRows(null, null,
                getFilteredCacheNodeColumnInfos(nodeFilter).size(), OpenCypherResultSetGetTables.getColumns());
    }
}
