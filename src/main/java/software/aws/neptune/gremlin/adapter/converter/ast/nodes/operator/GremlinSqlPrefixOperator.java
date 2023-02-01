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

package software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.gremlin.adapter.converter.SqlMetadata;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.logic.GremlinSqlBinaryOperator;

import java.sql.SQLException;
import java.util.List;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlPostFixOperator.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class GremlinSqlPrefixOperator extends GremlinSqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlPrefixOperator.class);
    private final SqlPrefixOperator sqlPrefixOperator;
    private final SqlMetadata sqlMetadata;
    private final List<GremlinSqlNode> sqlOperands;

    public GremlinSqlPrefixOperator(final SqlPrefixOperator sqlPrefixOperator,
                                    final List<GremlinSqlNode> gremlinSqlNodes,
                                    final SqlMetadata sqlMetadata) {
        super(sqlPrefixOperator, gremlinSqlNodes, sqlMetadata);
        this.sqlPrefixOperator = sqlPrefixOperator;
        this.sqlMetadata = sqlMetadata;
        this.sqlOperands = gremlinSqlNodes;
    }

    public String getNewName() throws SQLException {
        return String.format("%s %s", sqlPrefixOperator.kind.sql, getOperandName(sqlOperands.get(0)));
    }

    public boolean isNot() {
        return sqlPrefixOperator.kind.equals(SqlKind.NOT);
    }

    @Override
    protected void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        // If we are done our filtering, have a not operator, and have a single GremlinSqlBasicCall operand,
        // we can then use not(<graph_traversal>).
        if (sqlMetadata.isDoneFilters() && isNot() &&
                sqlOperands.size() == 1 && sqlOperands.get(0) instanceof GremlinSqlBasicCall) {
            final GremlinSqlBinaryOperator pseudoGremlinSqlBinaryOperator =
                    new GremlinSqlBinaryOperator(sqlPrefixOperator, sqlOperands, sqlMetadata);
            pseudoGremlinSqlBinaryOperator.appendTraversal(graphTraversal);
        }
    }

}
