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

import lombok.SneakyThrows;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.SqlToGremlin;
import org.twilmes.sql.gremlin.processor.SingleQueryExecutor;
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
import java.lang.reflect.Constructor;
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
    public SqlGremlinQueryExecutor(final GremlinConnectionProperties gremlinConnectionProperties) {
        super(gremlinConnectionProperties);
        this.gremlinConnectionProperties = gremlinConnectionProperties;
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
        if (sqlToGremlin == null) {
            sqlToGremlin = new SqlToGremlin(getSqlGremlinGraphSchema(gremlinConnectionProperties),
                    getGraphTraversalSource(gremlinConnectionProperties));
        }
        return sqlToGremlin;
    }

    // TODO AN-540: Look into a caching mechanism for this to improve performance when we have time to revisit this.

    /**
     * Function to get schema of graph in terms that sql-gremlin can understand.
     *
     * @param gremlinConnectionProperties Connection properties.
     * @return SchemaConfig Object for Graph.
     * @throws SQLException If issue is encountered and schema cannot be determined.
     */
    public static SchemaConfig getSqlGremlinGraphSchema(final GremlinConnectionProperties gremlinConnectionProperties)
            throws SQLException {
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
                    .getConstructor(java.sql.Statement.class, SingleQueryExecutor.SqlGremlinQueryResult.class);
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
        return (T) getSqlToGremlin(gremlinConnectionProperties).execute(query);
    }

    // TODO AN-540: Look into query cancellation.
    @Override
    protected void performCancel() {
    }
}
