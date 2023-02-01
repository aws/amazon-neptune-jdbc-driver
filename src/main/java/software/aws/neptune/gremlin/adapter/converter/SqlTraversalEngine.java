/*
 * Copyright <2023> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.gremlin.adapter.converter;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.select.StepDirection;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinTableBase;
import software.aws.neptune.gremlin.adapter.results.SqlGremlinQueryResult;
import software.aws.neptune.gremlin.adapter.util.SqlGremlinError;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinTableBase.IN_ID;
import static software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinTableBase.OUT_ID;

/**
 * Traversal engine for SQL-Gremlin. This module is responsible for generating the gremlin traversals.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class SqlTraversalEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTraversalEngine.class);

    public static GraphTraversal<?, ?> generateInitialSql(final List<GremlinSqlIdentifier> gremlinSqlIdentifiers,
                                                          final SqlMetadata sqlMetadata,
                                                          final GraphTraversalSource g) throws SQLException {
        if (gremlinSqlIdentifiers.size() != 2) {
            throw SqlGremlinError.create(SqlGremlinError.IDENTIFIER_SIZE_INCORRECT);
        }
        final String label = sqlMetadata.getActualTableName(gremlinSqlIdentifiers.get(0).getName(1));
        final GraphTraversal<?, ?> graphTraversal = sqlMetadata.isVertex(label) ? g.V() : g.E();
        graphTraversal.hasLabel(label);
        return graphTraversal;
    }

    public static void applyAggregateFold(final SqlMetadata sqlMetadata, final GraphTraversal<?, ?> graphTraversal) {
        if (sqlMetadata.getIsProjectFoldRequired()) {
            graphTraversal.fold();
        }
    }

    public static void applyAggregateUnfold(final SqlMetadata sqlMetadata, final GraphTraversal<?, ?> graphTraversal) {
        if (sqlMetadata.getIsProjectFoldRequired()) {
            graphTraversal.unfold();
        }
    }

    public static GraphTraversal<?, ?> getEmptyTraversal(final StepDirection direction, final SqlMetadata sqlMetadata) {
        final GraphTraversal<?, ?> graphTraversal = __.unfold();
        applyAggregateUnfold(sqlMetadata, graphTraversal);
        switch (direction) {
            case Out:
                return graphTraversal.outV();
            case In:
                return graphTraversal.inV();
        }
        return graphTraversal;
    }

    public static void addProjection(final List<GremlinSqlIdentifier> gremlinSqlIdentifiers,
                                     final SqlMetadata sqlMetadata,
                                     final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (gremlinSqlIdentifiers.size() != 2) {
            throw SqlGremlinError.create(SqlGremlinError.IDENTIFIER_SIZE_INCORRECT);
        }
        final String label = sqlMetadata.getActualTableName(gremlinSqlIdentifiers.get(0).getName(1));
        final String projectLabel = gremlinSqlIdentifiers.get(1).getName(0);

        graphTraversal.project(projectLabel);
        sqlMetadata.addRenamedTable(label, projectLabel);
    }

    public static GraphTraversal<?, ?> getEmptyTraversal(final SqlMetadata sqlMetadata) {
        return getEmptyTraversal(StepDirection.None, sqlMetadata);
    }


    public static void applyTraversal(GraphTraversal graphTraversal,
                                      final GraphTraversal subGraphTraversal,
                                      final boolean apply) {
            graphTraversal.by((apply ? __.coalesce(subGraphTraversal, __.constant(SqlGremlinQueryResult.NULL_VALUE)) : subGraphTraversal));
    }


    public static void applyTraversal(final GraphTraversal graphTraversal,
                                      final GraphTraversal subGraphTraversal) {
        applyTraversal(graphTraversal, subGraphTraversal, false);
    }

    public static void applySqlIdentifier(final GremlinSqlIdentifier sqlIdentifier,
                                          final SqlMetadata sqlMetadata,
                                          final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlIdentifier.isStar()) {
            // SELECT * will be fixed by calcite so that it is all the underlying nodes.
            // SELECT COUNT(*) on the other hand will actually just be replaced with an empty string.
            // So we need to inject something into our traversal for this.
            graphTraversal.constant(1);
        } else if (sqlIdentifier.getNameCount() == 2) {
            // With size 2 format of identifier is 'table'.'column' => ['table', 'column']
            appendGraphTraversal(sqlIdentifier.getName(0), sqlMetadata.getRenamedColumn(sqlIdentifier.getName(1)),
                    sqlMetadata, graphTraversal);
        } else {
            // With size 1, format of identifier is 'column'.
            appendGraphTraversal(sqlMetadata.getRenamedColumn(sqlIdentifier.getName(0)), sqlMetadata, graphTraversal);
        }
    }

    public static GraphTraversal<?, ?> applyColumnRenames(final List<String> columnsRenamed) throws SQLException {
        final String firstColumn = columnsRenamed.remove(0);
        final String[] remaining = columnsRenamed.toArray(new String[] {});
        return __.project(firstColumn, remaining);
    }

    private static void appendColumnSelect(final SqlMetadata sqlMetadata, final String column,
                                           final GraphTraversal<?, ?> graphTraversal) {
        if (sqlMetadata.isDoneFilters()) {
            graphTraversal.choose(__.has(column), __.values(column), __.constant(SqlGremlinQueryResult.NULL_VALUE));
        } else {
            graphTraversal.has(column).values(column);
        }
    }

    private static void appendGraphTraversal(final String table, final String column,
                                             final SqlMetadata sqlMetadata,
                                             final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        final GremlinTableBase gremlinTableBase = sqlMetadata.getGremlinTable(table);
        final String columnName = sqlMetadata.getActualColumnName(gremlinTableBase, column);

        // Primary/foreign key, need to traverse appropriately.
        if (!columnName.endsWith(GremlinTableBase.ID)) {
            if (sqlMetadata.getIsAggregate()) {
                graphTraversal.has(columnName).values(columnName);
            } else {
                appendColumnSelect(sqlMetadata, columnName, graphTraversal);
            }
        } else {
            // It's this vertex/edge.
            if (columnName.toLowerCase(Locale.getDefault())
                    .startsWith(gremlinTableBase.getLabel().toLowerCase(Locale.getDefault()))) {
                graphTraversal.id();
            } else {
                if (columnName.endsWith(IN_ID)) {
                    // Vertices can have many connected, edges (thus we need to fold). Edges can only connect to 1 vertex.
                    if (gremlinTableBase.getIsVertex()) {
                        graphTraversal.coalesce(__.inE().hasLabel(columnName.replace(IN_ID, "")).id().fold(),
                                __.constant(new ArrayList<>()));
                    } else {
                        graphTraversal.coalesce(__.inV().hasLabel(columnName.replace(IN_ID, "")).id(),
                                __.constant(new ArrayList<>()));
                    }
                } else if (column.endsWith(OUT_ID)) {
                    // Vertices can have many connected, edges (thus we need to fold). Edges can only connect to 1 vertex.
                    if (gremlinTableBase.getIsVertex()) {
                        graphTraversal.coalesce(__.outE().hasLabel(columnName.replace(OUT_ID, "")).id().fold(),
                                __.constant(new ArrayList<>()));
                    } else {
                        graphTraversal.coalesce(__.outV().hasLabel(columnName.replace(OUT_ID, "")).id(),
                                __.constant(new ArrayList<>()));
                    }
                } else {
                    graphTraversal.constant(new ArrayList<>());
                }
            }
        }
    }

    private static void appendGraphTraversal(final String columnName,
                                             final SqlMetadata sqlMetadata,
                                             final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        // Primary/foreign key, need to traverse appropriately.
        if (columnName.endsWith(GremlinTableBase.ID)) {
            throw SqlGremlinError.create(SqlGremlinError.ID_BASED_APPEND);
        }
        if (sqlMetadata.getIsAggregate()) {
            graphTraversal.has(columnName).values(columnName);
        } else {
            appendColumnSelect(sqlMetadata, columnName, graphTraversal);
        }
    }
}
