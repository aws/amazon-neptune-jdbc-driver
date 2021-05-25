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

import lombok.SneakyThrows;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.twilmes.sql.gremlin.SqlToGremlin;
import org.twilmes.sql.gremlin.processor.SingleQueryExecutor;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableColumn;
import org.twilmes.sql.gremlin.schema.TableConfig;
import org.twilmes.sql.gremlin.schema.TableRelationship;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.neptune.common.gremlindatamodel.GraphSchema;
import software.amazon.neptune.common.gremlindatamodel.MetadataCache;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * Implementation of QueryExecutor for SQL via Gremlin.
 */
public class SqlGremlinQueryExecutor extends GremlinQueryExecutor {
    private final GremlinConnectionProperties gremlinConnectionProperties;

    /**
     * Constructor for SqlGremlinQueryExecutor.
     * @param gremlinConnectionProperties GremlinConnectionProperties for connection.
     */
    public SqlGremlinQueryExecutor(final GremlinConnectionProperties gremlinConnectionProperties) {
        super(gremlinConnectionProperties);
        this.gremlinConnectionProperties = gremlinConnectionProperties;
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
        // TODO AN-542: Implement this.
        return null;
    }

    @SneakyThrows
    @Override
    @SuppressWarnings("unchecked")
    protected <T> T runQuery(final String query) throws SQLException {
        final GraphTraversalSource remoteConnection =
                traversal().withRemote(DriverRemoteConnection.using(getClient(gremlinConnectionProperties)));
        final SqlToGremlin sqlToGremlin = new SqlToGremlin(getSqlGremlinGraphSchema(), remoteConnection);
        final SingleQueryExecutor.SqlGremlinQueryResult queryResult = sqlToGremlin.execute(query);

        // TODO AN-542: Convert queryResult to JDBC style result.

        return null;
    }

    @Override
    protected void performCancel() throws SQLException {
    }

    // TODO AN-540: Look into a caching mechanism for this to improve performance when we have time to revisit this.

    /**
     * Function to get schema of graph in terms that sql-gremlin can understand.
     *
     * @return SchemaConfig Object for Graph.
     * @throws SQLException If issue is encountered and schema cannot be determined.
     */
    public SchemaConfig getSqlGremlinGraphSchema() throws SQLException {
        if (!MetadataCache.isMetadataCached()) {
            MetadataCache.updateCache(gremlinConnectionProperties.getContactPoint(), null,
                    (gremlinConnectionProperties.getAuthScheme() == AuthScheme.IAMSigV4),
                    MetadataCache.PathType.Gremlin);
        }

        final List<GraphSchema> nodeSchema = MetadataCache.getNodeSchemaList();
        final List<GraphSchema> edgeSchema = MetadataCache.getEdgeSchemaList();


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
            tableConfig.setName(String.join(":", graphSchema.getLabels()));
            tableConfig.setColumns(tableColumns);
            tableConfigList.add(tableConfig);
        }

        // Get Edge table information.
        final List<TableRelationship> tableRelationshipList = new ArrayList<>();
        for (final GraphSchema graphSchema : edgeSchema) {
            final TableRelationship tableRelationship = new TableRelationship();
            // TODO AN-540: Gremlin export tool does not extract this info. Need to revisit during Tableau integration.
            tableRelationship.setFkTable(null);
            tableRelationship.setInTable(null);
            tableRelationship.setOutTable(null);
            tableRelationship.setEdgeLabel(String.join(":", graphSchema.getLabels()));
            tableRelationshipList.add(tableRelationship);
        }

        final SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setTables(tableConfigList);
        schemaConfig.setRelationships(tableRelationshipList);

        return schemaConfig;
    }
}
