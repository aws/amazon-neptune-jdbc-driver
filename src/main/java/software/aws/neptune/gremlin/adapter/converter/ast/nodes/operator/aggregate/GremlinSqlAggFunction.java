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

package software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.aggregate;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import software.aws.neptune.gremlin.adapter.converter.SqlMetadata;
import software.aws.neptune.gremlin.adapter.converter.SqlTraversalEngine;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlBasicCall;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlOperator;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlTraversalAppender;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.logic.GremlinSqlLiteral;
import software.aws.neptune.gremlin.adapter.util.SqlGremlinError;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlAggFunction.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class GremlinSqlAggFunction extends GremlinSqlOperator {
    // See SqlKind.AGGREGATE for list of aggregate functions in Calcite.
    private static final Map<SqlKind, GremlinSqlTraversalAppender> AGGREGATE_APPENDERS =
            new HashMap<SqlKind, GremlinSqlTraversalAppender>() {{
                put(SqlKind.AVG, GremlinSqlAggFunctionImplementations.AVG);
                put(SqlKind.COUNT, GremlinSqlAggFunctionImplementations.COUNT);
                put(SqlKind.SUM, GremlinSqlAggFunctionImplementations.SUM);
                put(SqlKind.MIN, GremlinSqlAggFunctionImplementations.MIN);
                put(SqlKind.MAX, GremlinSqlAggFunctionImplementations.MAX);
            }};
    private static final Map<SqlKind, String> AGGREGATE_TYPE_MAP =
            new HashMap<SqlKind, String>() {{
                put(SqlKind.AVG, "double");
                put(SqlKind.COUNT, "long");
            }};
    private final SqlAggFunction sqlAggFunction;
    private final SqlMetadata sqlMetadata;
    private final List<GremlinSqlNode> sqlOperands;


    public GremlinSqlAggFunction(final SqlAggFunction sqlOperator,
                                 final List<GremlinSqlNode> gremlinSqlNodes,
                                 final SqlMetadata sqlMetadata) {
        super(sqlOperator, gremlinSqlNodes, sqlMetadata);
        this.sqlAggFunction = sqlOperator;
        this.sqlMetadata = sqlMetadata;
        this.sqlOperands = gremlinSqlNodes;
    }

    @Override
    protected void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlOperands.get(0) instanceof GremlinSqlBasicCall) {
            ((GremlinSqlBasicCall) sqlOperands.get(0)).generateTraversal(graphTraversal);
        } else if (!(sqlOperands.get(0) instanceof GremlinSqlIdentifier) &&
                !(sqlOperands.get(0) instanceof GremlinSqlLiteral)) {
            throw SqlGremlinError.create(SqlGremlinError.UNEXPECTED_OPERAND);
        }

        if (sqlOperands.size() == 1) {
            if (sqlOperands.get(0) instanceof GremlinSqlIdentifier) {
                SqlTraversalEngine.applySqlIdentifier((GremlinSqlIdentifier) sqlOperands.get(0), sqlMetadata,
                        graphTraversal);
            } else if (sqlOperands.get(0) instanceof GremlinSqlLiteral) {
                GremlinSqlLiteral gremlinSqlLiteral = (GremlinSqlLiteral) sqlOperands.get(0);
                gremlinSqlLiteral.appendTraversal(graphTraversal);
            }
        }
        if (AGGREGATE_APPENDERS.containsKey(sqlAggFunction.kind)) {
            AGGREGATE_APPENDERS.get(sqlAggFunction.kind).appendTraversal(graphTraversal, sqlOperands);
        } else {
            throw SqlGremlinError.create(SqlGremlinError.AGGREGATE_NOT_SUPPORTED, sqlAggFunction.kind.sql);
        }
        updateOutputTypeMap();
    }

    /**
     * Aggregation columns will be named in the form of AGG(xxx) if no rename is specified in SQL
     */
    public String getNewName() throws SQLException {
        if (sqlOperands.get((sqlOperands.size() - 1)) instanceof GremlinSqlIdentifier) {
            final GremlinSqlIdentifier gremlinSqlIdentifier =
                    (GremlinSqlIdentifier) sqlOperands.get((sqlOperands.size() - 1));
            return String.format("%s(%s)", sqlAggFunction.kind.name(),
                    gremlinSqlIdentifier.isStar() ? "*" : gremlinSqlIdentifier.getColumn());
        } else if (sqlOperands.get((sqlOperands.size() - 1)) instanceof GremlinSqlLiteral) {
            return String.format("%s(%s)", sqlAggFunction.kind.name(),
                    ((GremlinSqlLiteral) sqlOperands.get(sqlOperands.size() - 1)).getValue().toString());
        }
        throw SqlGremlinError.create(SqlGremlinError.FAILED_RENAME_GREMLINSQLAGGOPERATOR);
    }

    public String getActual() throws SQLException {
        if (sqlOperands.get((sqlOperands.size() - 1)) instanceof GremlinSqlIdentifier) {
            return ((GremlinSqlIdentifier) sqlOperands.get(sqlOperands.size() - 1)).getColumn();
        } else if (sqlOperands.get((sqlOperands.size() - 1)) instanceof GremlinSqlLiteral) {
            return ((GremlinSqlLiteral) sqlOperands.get(sqlOperands.size() - 1)).getValue().toString();
        }
        throw SqlGremlinError.create(SqlGremlinError.FAILED_RENAME_GREMLINSQLAGGOPERATOR);
    }

    private void updateOutputTypeMap() throws SQLException {
        if (AGGREGATE_TYPE_MAP.containsKey(sqlAggFunction.kind)) {
            sqlMetadata.addOutputType(getNewName(), AGGREGATE_TYPE_MAP.get(sqlAggFunction.kind));
        }
        sqlMetadata.addRenamedColumn(getActual(), getNewName());
    }

    private static class GremlinSqlAggFunctionImplementations {
        public static final GremlinSqlTraversalAppender AVG =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> graphTraversal.mean();
        public static final GremlinSqlTraversalAppender COUNT =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> graphTraversal.count();
        public static final GremlinSqlTraversalAppender SUM =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> graphTraversal.sum();
        public static final GremlinSqlTraversalAppender MIN =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> graphTraversal.min();
        public static final GremlinSqlTraversalAppender MAX =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> graphTraversal.max();
    }
}
