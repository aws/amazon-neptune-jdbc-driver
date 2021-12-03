/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.twilmes.sql.gremlin.adapter.converter.ast.nodes;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlAsOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlBasicCall;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlPostfixOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlPrefixOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.aggregate.GremlinSqlAggFunction;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.logic.GremlinSqlBinaryOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.logic.GremlinSqlLiteral;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.select.GremlinSqlSelect;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.select.GremlinSqlSelectMulti;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.select.GremlinSqlSelectSingle;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.select.join.GremlinSqlJoinComparison;
import org.twilmes.sql.gremlin.adapter.util.SqlGremlinError;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This factory converts different types of Calcite's SqlNode/SqlOperator's to SqlGremlin equivalents.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 * @author Adapted from implementation by twilmes (https://github.com/twilmes/sql-gremlin)
 */
public class GremlinSqlFactory {
    private static SqlMetadata sqlMetadata = null;

    public static void setSqlMetadata(final SqlMetadata sqlMetadata1) {
        sqlMetadata = sqlMetadata1;
    }

    public static SqlMetadata getGremlinSqlMetadata() throws SQLException {
        if (sqlMetadata == null) {
            throw SqlGremlinError.create(SqlGremlinError.SCHEMA_NOT_SET);
        }
        return sqlMetadata;
    }

    public static GremlinSqlJoinComparison createJoinEquality(final SqlNode sqlNode)
            throws SQLException {
        if (sqlNode instanceof SqlBasicCall) {
            final SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator() instanceof SqlBinaryOperator) {
                return new GremlinSqlJoinComparison((SqlBasicCall) sqlNode,
                        (SqlBinaryOperator) sqlBasicCall.getOperator(), createNodeList(sqlBasicCall.getOperandList()),
                        getGremlinSqlMetadata());
            }
        }
        throw SqlGremlinError.create(SqlGremlinError.UNKNOWN_NODE, sqlNode.getClass().getName());
    }

    public static GremlinSqlOperator createOperator(final SqlOperator sqlOperator, final List<SqlNode> sqlOperands)
            throws SQLException {
        if (sqlOperator instanceof SqlAsOperator) {
            return new GremlinSqlAsOperator((SqlAsOperator) sqlOperator, createNodeList(sqlOperands),
                    getGremlinSqlMetadata());
        } else if (sqlOperator instanceof SqlAggFunction) {
            return new GremlinSqlAggFunction((SqlAggFunction) sqlOperator, createNodeList(sqlOperands),
                    getGremlinSqlMetadata());
        } else if (sqlOperator instanceof SqlBinaryOperator) {
            return new GremlinSqlBinaryOperator((SqlBinaryOperator) sqlOperator, createNodeList(sqlOperands),
                    getGremlinSqlMetadata());
        } else if (sqlOperator instanceof SqlPostfixOperator) {
            return new GremlinSqlPostfixOperator((SqlPostfixOperator) sqlOperator, createNodeList(sqlOperands),
                    getGremlinSqlMetadata());
        } else if (sqlOperator instanceof SqlPrefixOperator) {
            return new GremlinSqlPrefixOperator((SqlPrefixOperator) sqlOperator, createNodeList(sqlOperands),
                    getGremlinSqlMetadata());
        }
        throw SqlGremlinError.create(SqlGremlinError.UNKNOWN_OPERATOR, sqlOperator.getKind().sql);
    }

    public static GremlinSqlNode createNode(final SqlNode sqlNode) throws SQLException {
        if (sqlNode instanceof SqlBasicCall) {
            return new GremlinSqlBasicCall((SqlBasicCall) sqlNode, getGremlinSqlMetadata());
        } else if (sqlNode instanceof SqlIdentifier) {
            return new GremlinSqlIdentifier((SqlIdentifier) sqlNode, getGremlinSqlMetadata());
        } else if (sqlNode instanceof SqlLiteral) {
            return new GremlinSqlLiteral((SqlLiteral) sqlNode, getGremlinSqlMetadata());
        }
        throw SqlGremlinError.create(SqlGremlinError.UNKNOWN_NODE, sqlNode.getClass().getName());
    }

    public static List<GremlinSqlNode> createNodeList(final List<SqlNode> sqlNodes) throws SQLException {
        final List<GremlinSqlNode> gremlinSqlNodes = new ArrayList<>();
        for (final SqlNode sqlNode : sqlNodes) {
            gremlinSqlNodes.add(createNode(sqlNode));
        }
        return gremlinSqlNodes;
    }

    @SuppressWarnings("unchecked")
    public static <T> T createNodeCheckType(final SqlNode sqlNode, final Class<T> clazz) throws SQLException {
        final GremlinSqlNode gremlinSqlNode = createNode(sqlNode);
        if (!gremlinSqlNode.getClass().equals(clazz)) {
            throw SqlGremlinError.create(SqlGremlinError.TYPE_MISMATCH);
        }
        return (T) gremlinSqlNode;
    }

    public static GremlinSqlSelect createSelect(final SqlSelect selectRoot, final GraphTraversalSource g)
            throws SQLException {
        if (selectRoot.getFrom() instanceof SqlJoin) {
            return new GremlinSqlSelectMulti(selectRoot, (SqlJoin) selectRoot.getFrom(), sqlMetadata, g);
        } else if (selectRoot.getFrom() instanceof SqlBasicCall) {
            return new GremlinSqlSelectSingle(selectRoot, (SqlBasicCall) selectRoot.getFrom(), sqlMetadata, g);
        }
        throw SqlGremlinError.create(SqlGremlinError.UNKNOWN_NODE_GETFROM, selectRoot.getFrom().getClass().getName());
    }

    public static boolean isTable(final SqlNode sqlNode, final String renamedTable) throws SQLException {
        if (sqlNode instanceof SqlIdentifier) {
            return (((SqlIdentifier) sqlNode).names.get(0).equalsIgnoreCase(renamedTable));
        } else if (sqlNode instanceof SqlCall) {
            for (final SqlNode tmpSqlNode : ((SqlCall) sqlNode).getOperandList()) {
                if (isTable(tmpSqlNode, renamedTable)) {
                    return true;
                }
            }
        } else {
            throw SqlGremlinError.create(SqlGremlinError.UNKNOWN_NODE_ISTABLE);
        }
        return false;
    }
}
