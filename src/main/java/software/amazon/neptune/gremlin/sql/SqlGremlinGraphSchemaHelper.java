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

package software.amazon.neptune.gremlin.sql;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableColumn;
import org.twilmes.sql.gremlin.schema.TableConfig;
import org.twilmes.sql.gremlin.schema.TableRelationship;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import software.amazon.neptune.common.gremlindatamodel.GraphSchema;
import software.amazon.neptune.common.gremlindatamodel.MetadataCache;
import software.amazon.neptune.gremlin.GremlinConnectionProperties;
import software.amazon.neptune.gremlin.GremlinQueryExecutor;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class SqlGremlinGraphSchemaHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlGremlinGraphSchemaHelper.class);

    static SchemaConfig getSchemaConfig(final GremlinConnectionProperties gremlinConnectionProperties)
            throws SQLException {
        if (!MetadataCache.isMetadataCached()) {
            MetadataCache.updateCache(gremlinConnectionProperties.getContactPoint(), null,
                    (gremlinConnectionProperties.getAuthScheme() == AuthScheme.IAMSigV4),
                    MetadataCache.PathType.Gremlin);
        }
        final SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setTables(getTableConfigs(MetadataCache.getNodeSchemaList()));
        schemaConfig.setRelationships(
                getTableRelationships(MetadataCache.getEdgeSchemaList(), gremlinConnectionProperties));
        return schemaConfig;
    }

    static List<TableConfig> getTableConfigs(final List<GraphSchema> nodeSchema) {
        // Get Node table information.
        final List<TableConfig> tableConfigList = new ArrayList<>();
        for (final GraphSchema graphSchema : nodeSchema) {
            final TableConfig tableConfig = new TableConfig();
            final List<TableColumn> tableColumns = new ArrayList<>();
            for (final Map<String, Object> properties : graphSchema.getProperties()) {
                final TableColumn tableColumn = new TableColumn();
                tableColumn.setType(properties.get("dataType").toString().toLowerCase());
                tableColumn.setName((String) properties.get("property"));
                tableColumn.setPropertyName(null);
                tableColumns.add(tableColumn);
            }

            // might be the issue
            tableConfig.setName(String.join(":", graphSchema.getLabels()));
            tableConfig.setColumns(tableColumns);
            tableConfigList.add(tableConfig);
        }
        return tableConfigList;
    }

    // TODO: This code needs to be cleaned up
    static List<TableRelationship> getTableRelationships(final List<GraphSchema> edgeSchema,
                                                         final GremlinConnectionProperties gremlinConnectionProperties)
            throws SQLException {
        final List<TableRelationship> tableRelationshipList = new ArrayList<>();
        for (final GraphSchema graphSchema : edgeSchema) {
            final InOutQueries inOutQueries = getInVertex(graphSchema.getLabels());
            final InOutLabels inOutLabels = runInOutQueries(gremlinConnectionProperties, inOutQueries);

            final TableRelationship tableRelationship = new TableRelationship();

            // Handle columns
            final List<TableColumn> tableColumns = new ArrayList<>();
            for (final Map<String, Object> properties : graphSchema.getProperties()) {
                final TableColumn tableColumn = new TableColumn();
                tableColumn.setType(properties.get("dataType").toString().toLowerCase());
                tableColumn.setName((String) properties.get("property"));
                tableColumn.setPropertyName(null);
                tableColumns.add(tableColumn);
            }
            tableRelationship.setColumns(tableColumns);

            // TODO: Investigate how to support this / how this is returned.
            // It is currently unclear if a given edge will return multiple labels if
            //  a) it is connected to vertexes that have different single labels
            //  b) it is connected to a vertex that has multiple labels
            //  c) both of the above
            // Need to figure this out and also do some handling around this. Perhaps we can leverage the vertex schema
            // that the neptune export tool gives us. For now hardcode and assume/handle the simplest case of a single
            // vertex.
            if (inOutLabels.getIn().size() > 1) {
                LOGGER.warn("Multiple in vertex labels not currently supported here, only first is used.");
            }
            if (inOutLabels.getOut().size() > 1) {
                LOGGER.warn("Multiple out vertex labels not currently supported here, only first is used.");
            }
            // TODO: Revisit this null value later.
            // If FkTable is null, it defaults to OutTable.
            tableRelationship.setFkTable(null);
            tableRelationship.setInTable(inOutLabels.getIn().get(0));
            tableRelationship.setOutTable(inOutLabels.getOut().get(0));
            tableRelationship.setEdgeLabel(String.join(":", graphSchema.getLabels()));
            tableRelationshipList.add(tableRelationship);
        }
        return tableRelationshipList;
    }

    static InOutQueries getInVertex(final List<String> labels) {
        // Combine labels into list of comma delimited labels.
        // ["foo", "bar", "baz"] -> "'foo', 'bar', 'baz'"
        // ["foo"] -> "'foo'"
        final String relationshipLabel = labels.stream().collect(Collectors.joining("','", "'", "'"));
        return new InOutQueries(getVertexLabelQuery("in", relationshipLabel),
                getVertexLabelQuery("out", relationshipLabel));
    }

    static String getVertexLabelQuery(final String direction, final String relationshipLabel) {
        // g.V().in(<labels with comma delimits and single quotes>).label().dedup()
        return String.format("g.V().%s(%s).label().dedup()", direction, relationshipLabel);
    }

    static InOutLabels runInOutQueries(final GremlinConnectionProperties gremlinConnectionProperties,
                                       final InOutQueries inOutQueries)
            throws SQLException {
        try {
            final Cluster cluster = GremlinQueryExecutor.createClusterBuilder(gremlinConnectionProperties).create();
            final Client client = cluster.connect().init();

            // Run queries async so they run in parallel.
            final CompletableFuture<ResultSet> inResult = client.submitAsync(inOutQueries.getIn());
            final CompletableFuture<ResultSet> outResult =
                    client.submitAsync(inOutQueries.getOut());

            // Block on in result and transform it to a List of Strings.
            final List<String> inLabels = inResult.get().stream().map(Result::getString).collect(Collectors.toList());

            // Block on out result and transform it to a List of Strings.
            final List<String> outLabels = outResult.get().stream().map(Result::getString).collect(Collectors.toList());
            return new InOutLabels(inLabels, outLabels);
        } catch (final Exception e) {
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.QUERY_FAILED, e);
        }
    }

    @AllArgsConstructor
    @Getter
    private static class InOutQueries {
        private final String in;
        private final String out;
    }

    @AllArgsConstructor
    @Getter
    private static class InOutLabels {
        private final List<String> in;
        private final List<String> out;
    }
}
