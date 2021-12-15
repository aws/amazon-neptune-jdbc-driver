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

package org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.SqlTraversalEngine;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.logic.GremlinSqlLiteral;
import org.twilmes.sql.gremlin.adapter.util.SqlGremlinError;

import java.sql.SQLException;
import java.util.List;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlPostFixOperator.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class GremlinSqlPostfixOperator extends GremlinSqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlPostfixOperator.class);
    private final SqlPostfixOperator sqlPostfixOperator;
    private final SqlMetadata sqlMetadata;
    private final List<GremlinSqlNode> sqlOperands;

    public GremlinSqlPostfixOperator(final SqlPostfixOperator sqlPostfixOperator, final List<GremlinSqlNode> gremlinSqlNodes,
                                     final SqlMetadata sqlMetadata) {
        super(sqlPostfixOperator, gremlinSqlNodes, sqlMetadata);
        this.sqlPostfixOperator = sqlPostfixOperator;
        this.sqlMetadata = sqlMetadata;
        this.sqlOperands = gremlinSqlNodes;
    }

    public String getNewName() throws SQLException {
        return String.format("%s %s", getOperandName(sqlOperands.get(0)), sqlPostfixOperator.kind.sql);
    }

    public Order getOrder() throws SQLException {
        if (sqlPostfixOperator.kind.equals(SqlKind.DESCENDING)) {
            return Order.desc;
        }
        throw SqlGremlinError.create(SqlGremlinError.NO_ORDER, sqlPostfixOperator.kind.sql);
    }

    @Override
    protected void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlOperands.get(0) instanceof GremlinSqlBasicCall) {
            ((GremlinSqlBasicCall) sqlOperands.get(0)).generateTraversal(graphTraversal);
        } else if (!(sqlOperands.get(0) instanceof GremlinSqlIdentifier) && !(sqlOperands.get(0) instanceof GremlinSqlLiteral)) {
            throw SqlGremlinError.create(SqlGremlinError.UNEXPECTED_OPERAND);
        }

        if (sqlOperands.size() == 1) {
            if (sqlOperands.get(0) instanceof GremlinSqlIdentifier) {
                SqlTraversalEngine
                        .applySqlIdentifier((GremlinSqlIdentifier) sqlOperands.get(0), sqlMetadata, graphTraversal);
            }
        }
        if (sqlOperands.size() == 2 && sqlOperands.get(0) instanceof GremlinSqlIdentifier) {
            SqlTraversalEngine.applySqlIdentifier((GremlinSqlIdentifier) sqlOperands.get(0), sqlMetadata, graphTraversal);
        }

    }

}
