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

package software.aws.neptune.gremlin.adapter.converter.ast.nodes.select;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.gremlin.adapter.converter.SqlMetadata;
import software.aws.neptune.gremlin.adapter.converter.SqlTraversalEngine;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.GremlinSqlFactory;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlAsOperator;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlBasicCall;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlOperator;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlPostfixOperator;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.logic.GremlinSqlBinaryOperator;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.logic.GremlinSqlLiteral;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinTableBase;
import software.aws.neptune.gremlin.adapter.results.SqlGremlinQueryResult;
import software.aws.neptune.gremlin.adapter.results.pagination.Pagination;
import software.aws.neptune.gremlin.adapter.results.pagination.SimpleDataReader;
import software.aws.neptune.gremlin.adapter.util.SqlGremlinError;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlSelect for a non-JOIN operation.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class GremlinSqlSelectSingle extends GremlinSqlSelect {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlSelectSingle.class);
    private final SqlSelect sqlSelect;
    private final SqlMetadata sqlMetadata;
    private final GraphTraversalSource g;
    private final SqlBasicCall sqlBasicCall;

    public GremlinSqlSelectSingle(final SqlSelect sqlSelect,
                                  final SqlBasicCall sqlBasicCall,
                                  final SqlMetadata sqlMetadata, final GraphTraversalSource g) {
        super(sqlSelect, sqlMetadata, g);
        this.sqlSelect = sqlSelect;
        this.sqlMetadata = sqlMetadata;
        this.g = g;
        this.sqlBasicCall = sqlBasicCall;
    }

    @Override
    protected void runTraversalExecutor(final GraphTraversal<?, ?> graphTraversal,
                                        final SqlGremlinQueryResult sqlGremlinQueryResult) throws SQLException {
        // Launch thread to continue grabbing results.
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("Data-Insert-Thread-%d").setDaemon(true).build());
        final List<List<String>> columns = new ArrayList<>(sqlMetadata.getColumnOutputListMap().values());
        if (columns.size() != 1) {
            throw SqlGremlinError.create(SqlGremlinError.SINGLE_SELECT_MULTI_RETURN);
        }
        executor.execute(new Pagination(new SimpleDataReader(
                sqlMetadata.getRenameFromActual(sqlMetadata.getTables().iterator().next().getLabel()), columns.get(0)),
                graphTraversal, sqlGremlinQueryResult));
        executor.shutdown();
    }

    @Override
    public GraphTraversal<?, ?> generateTraversal() throws SQLException {
        if (sqlSelect.getSelectList() == null) {
            throw SqlGremlinError.create(SqlGremlinError.SELECT_NO_LIST);
        }

        final GremlinSqlOperator gremlinSqlOperator =
                GremlinSqlFactory.createOperator(sqlBasicCall.getOperator(), sqlBasicCall.getOperandList());
        if (!(gremlinSqlOperator instanceof GremlinSqlAsOperator)) {
            throw SqlGremlinError.create(SqlGremlinError.UNEXPECTED_FROM_FORMAT);
        }
        final List<GremlinSqlNode> gremlinSqlOperands = GremlinSqlFactory.createNodeList(sqlBasicCall.getOperandList());
        final List<GremlinSqlIdentifier> gremlinSqlIdentifiers = new ArrayList<>();
        for (final GremlinSqlNode gremlinSqlOperand : gremlinSqlOperands) {
            if (!(gremlinSqlOperand instanceof GremlinSqlIdentifier)) {
                throw SqlGremlinError.create(SqlGremlinError.UNEXPECTED_FROM_FORMAT);
            }
            gremlinSqlIdentifiers.add((GremlinSqlIdentifier) gremlinSqlOperand);
        }

        GraphTraversal<?, ?> graphTraversal = null;
        try {
            graphTraversal =
                    SqlTraversalEngine.generateInitialSql(gremlinSqlIdentifiers, sqlMetadata, g);
            final String label = sqlMetadata.getActualTableName(gremlinSqlIdentifiers.get(0).getName(1));

            // This function basically generates the latter parts of the traversal, by doing this it prepares all the
            // renamed labels in the metadata so that queries like 'SELECT foo AS bar FROM baz ORDER BY bar'
            // can properly recognize that bar=>foo.
            // __.__() is passed in as an anonymous traversal that will be discarded.
            generateDataRetrieval(gremlinSqlIdentifiers, __.__());

            // Generate actual traversal.
            applyWhere(graphTraversal);
            applyGroupBy(graphTraversal, label);
            applySelectValues(graphTraversal);
            applyOrderBy(graphTraversal, label);
            applyHaving(graphTraversal);
            sqlMetadata.setIsDoneFilters(true);
            generateDataRetrieval(gremlinSqlIdentifiers, graphTraversal);

            if (sqlMetadata.getRenamedColumns() == null) {
                throw SqlGremlinError.create(SqlGremlinError.COLUMN_RENAME_LIST_EMPTY);
            }
            if (sqlMetadata.getTables().size() != 1) {
                throw SqlGremlinError.create(SqlGremlinError.NO_TRAVERSAL_TABLE);
            }
            return graphTraversal;
        } catch (final SQLException e) {
            if (graphTraversal != null) {
                try {
                    graphTraversal.close();
                } catch (final Exception ignored) {
                }
            }
            throw e;
        }
    }

    private void generateDataRetrieval(final List<GremlinSqlIdentifier> gremlinSqlIdentifiers,
                                       GraphTraversal<?, ?> graphTraversal) throws SQLException {
        final String projectLabel = gremlinSqlIdentifiers.get(1).getName(0);

        final GraphTraversal<?, Map<String, ?>> graphTraversalDataPath = __.__();
        SqlTraversalEngine.addProjection(gremlinSqlIdentifiers, sqlMetadata, graphTraversalDataPath);
        applyColumnRetrieval(graphTraversalDataPath, projectLabel,
                GremlinSqlFactory.createNodeList(sqlSelect.getSelectList().getList()));

        SqlTraversalEngine.applyAggregateFold(sqlMetadata, graphTraversal);
        final GraphTraversal<?, ?> graphTraversalChoosePredicate = __.unfold();
        SqlTraversalEngine.applyAggregateUnfold(sqlMetadata, graphTraversalChoosePredicate);
        graphTraversal.choose(graphTraversalChoosePredicate, graphTraversalDataPath, __.__());
    }

    public String getStringTraversal() throws SQLException {
        return GroovyTranslator.of("g").translate(generateTraversal().asAdmin().getBytecode()).toString();
    }

    private void applySelectValues(final GraphTraversal<?, ?> graphTraversal) {
        graphTraversal.select(Column.values);
    }

    protected void applyGroupBy(final GraphTraversal<?, ?> graphTraversal, final String table) throws SQLException {
        if ((sqlSelect.getGroup() == null) || (sqlSelect.getGroup().getList().isEmpty())) {
            // If we group bys but we have aggregates, we need to shove things into groups by ourselves.-
            graphTraversal.group().unfold();
        } else {
            final List<GremlinSqlNode> gremlinSqlNodes = new ArrayList<>();
            for (final SqlNode sqlNode : sqlSelect.getGroup().getList()) {
                gremlinSqlNodes.add(GremlinSqlFactory.createNodeCheckType(sqlNode, GremlinSqlIdentifier.class));
            }
            graphTraversal.group();
            final List<GraphTraversal> byUnion = new ArrayList<>();
            for (final GremlinSqlNode gremlinSqlNode : gremlinSqlNodes) {
                final GraphTraversal graphTraversal1 = __.__();
                toAppendToByGraphTraversal(gremlinSqlNode, table, graphTraversal1);
                byUnion.add(graphTraversal1);
            }
            graphTraversal.by(__.union(byUnion.toArray(new GraphTraversal[0])).fold()).unfold();
        }
    }

    protected void applyOrderBy(final GraphTraversal<?, ?> graphTraversal, final String table) throws SQLException {
        graphTraversal.order();
        if (sqlSelect.getOrderList() == null || sqlSelect.getOrderList().getList().isEmpty()) {
            graphTraversal.by(__.unfold().id());
            return;
        }
        final List<GremlinSqlNode> gremlinSqlIdentifiers = new ArrayList<>();
        for (final SqlNode sqlNode : sqlSelect.getOrderList().getList()) {
            gremlinSqlIdentifiers.add(GremlinSqlFactory.createNode(sqlNode));
        }
        for (final GremlinSqlNode gremlinSqlNode : gremlinSqlIdentifiers) {
            appendByGraphTraversal(gremlinSqlNode, table, graphTraversal);
        }
    }

    private void toAppendToByGraphTraversal(final GremlinSqlNode gremlinSqlNode, final String table,
                                            final GraphTraversal graphTraversal)
            throws SQLException {
        if (gremlinSqlNode instanceof GremlinSqlIdentifier) {
            final String column = sqlMetadata
                    .getActualColumnName(sqlMetadata.getGremlinTable(table),
                            ((GremlinSqlIdentifier) gremlinSqlNode).getColumn());
            if (column.endsWith(GremlinTableBase.IN_ID) || column.endsWith(GremlinTableBase.OUT_ID)) {
                // TODO: Grouping edges that are not the edge that the vertex are connected - needs to be implemented.
                throw SqlGremlinError.create(SqlGremlinError.CANNOT_GROUP_EDGES);
            } else {
                graphTraversal.values(sqlMetadata.getActualColumnName(sqlMetadata.getGremlinTable(table), column));
            }
        } else if (gremlinSqlNode instanceof GremlinSqlBasicCall) {
            final GremlinSqlBasicCall gremlinSqlBasicCall = (GremlinSqlBasicCall) gremlinSqlNode;
            gremlinSqlBasicCall.generateTraversal(graphTraversal);
        }
    }

    private void appendByGraphTraversal(final GremlinSqlNode gremlinSqlNode, final String table,
                                        final GraphTraversal graphTraversal)
            throws SQLException {
        final GraphTraversal graphTraversal1 = __.unfold();
        if (gremlinSqlNode instanceof GremlinSqlIdentifier) {
            final String column = sqlMetadata
                    .getActualColumnName(sqlMetadata.getGremlinTable(table),
                            ((GremlinSqlIdentifier) gremlinSqlNode).getColumn());
            if (column.endsWith(GremlinTableBase.IN_ID) || column.endsWith(GremlinTableBase.OUT_ID)) {
                // TODO: Grouping edges that are not the edge that the vertex are connected - needs to be implemented.
                throw SqlGremlinError.create(SqlGremlinError.CANNOT_GROUP_EDGES);
            } else {
                graphTraversal1.values(sqlMetadata.getActualColumnName(sqlMetadata.getGremlinTable(table), column));
            }
            graphTraversal.by(__.coalesce(graphTraversal1, __.constant(sqlMetadata.getDefaultCoalesceValue(column))));
        } else if (gremlinSqlNode instanceof GremlinSqlBasicCall) {
            final GremlinSqlBasicCall gremlinSqlBasicCall = (GremlinSqlBasicCall) gremlinSqlNode;
            gremlinSqlBasicCall.generateTraversal(graphTraversal1);
            if (gremlinSqlBasicCall.getGremlinSqlOperator() instanceof GremlinSqlPostfixOperator) {
                final GremlinSqlPostfixOperator gremlinSqlPostFixOperator =
                        (GremlinSqlPostfixOperator) gremlinSqlBasicCall.getGremlinSqlOperator();
                graphTraversal.by(__.coalesce(graphTraversal1,
                                __.constant(sqlMetadata.getDefaultCoalesceValue(gremlinSqlBasicCall.getOutputColumn()))),
                        gremlinSqlPostFixOperator.getOrder());
            } else {
                graphTraversal.by(__.coalesce(graphTraversal1,
                        __.constant(sqlMetadata.getDefaultCoalesceValue(gremlinSqlBasicCall.getOutputColumn()))));
            }
        } else if (gremlinSqlNode instanceof GremlinSqlLiteral) {
            final GremlinSqlLiteral gremlinSqlLiteral = (GremlinSqlLiteral) gremlinSqlNode;
            final List<SqlNode> sqlNodeList = sqlSelect.getSelectList().getList();
            if (gremlinSqlLiteral.getValue() instanceof Number) {
                final Number value = (Number) gremlinSqlLiteral.getValue();
                if (sqlNodeList.size() <= value.intValue() || value.intValue() <= 0) {
                    appendByGraphTraversal(GremlinSqlFactory.createNode(sqlNodeList.get(value.intValue() - 1)), table,
                            graphTraversal);
                } else {
                    throw SqlGremlinError.create(SqlGremlinError.ORDER_BY_ORDINAL_VALUE);
                }
            } else {
                throw SqlGremlinError.create(SqlGremlinError.CANNOT_ORDER_COLUMN_LITERAL);
            }
        } else {
            throw SqlGremlinError.createNotSupported(SqlGremlinError.CANNOT_ORDER_BY,
                    gremlinSqlNode.getClass().getName());
        }
    }

    protected void applyHaving(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        applySqlFilter(sqlSelect.getHaving(), graphTraversal);
    }

    protected void applyWhere(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        applySqlFilter(sqlSelect.getWhere(), graphTraversal);
    }

    void applySqlFilter(SqlNode sqlNode, GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlNode == null) {
            return;
        }

        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator() instanceof SqlPrefixOperator) {
                SqlPrefixOperator sqlPrefixOperator = (SqlPrefixOperator) sqlBasicCall.getOperator();
                if (sqlPrefixOperator.kind.equals(SqlKind.NOT)) {
                    if (sqlBasicCall.getOperandList().size() == 1 && sqlBasicCall.getOperandList().size() == 1) {
                        final GraphTraversal<?, ?> subGraphTraversal = __.__();
                        applySqlFilter(sqlBasicCall.getOperandList().get(0), subGraphTraversal);
                        graphTraversal.not(subGraphTraversal);
                        return;
                    }
                    throw SqlGremlinError.createNotSupported(SqlGremlinError.WHERE_NOT_ONLY_BOOLEAN);
                }
                throw SqlGremlinError.createNotSupported(SqlGremlinError.WHERE_UNSUPPORTED_PREFIX);
            }
            GremlinSqlFactory.createNodeCheckType(sqlNode, GremlinSqlBasicCall.class)
                    .generateTraversal(graphTraversal);
            return;
        } else if (sqlNode instanceof SqlIdentifier) {
            GremlinSqlBinaryOperator.appendBooleanEquals(sqlMetadata, graphTraversal,
                    GremlinSqlFactory.createNodeCheckType(sqlNode, GremlinSqlIdentifier.class), true);
            return;
        }
        throw SqlGremlinError.createNotSupported(SqlGremlinError.WHERE_BASIC_LITERALS);
    }
}
