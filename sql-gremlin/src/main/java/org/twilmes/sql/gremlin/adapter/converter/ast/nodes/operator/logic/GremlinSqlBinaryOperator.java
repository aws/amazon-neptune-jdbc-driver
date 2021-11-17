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

package org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.logic;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.SqlTraversalEngine;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlBasicCall;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlPrefixOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlTraversalAppender;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * This module is a GremlinSql equivalent of Calcite's GremlinSqlBinaryOperator.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class GremlinSqlBinaryOperator extends GremlinSqlOperator {
    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlBinaryOperator.class);
    private final Map<SqlKind, GremlinSqlTraversalAppender> BINARY_APPENDERS =
            new HashMap<SqlKind, GremlinSqlTraversalAppender>() {
                {
                    put(SqlKind.EQUALS, new GremlinSqlBinaryOperatorAppenderEquals());
                    put(SqlKind.NOT_EQUALS, new GremlinSqlBinaryOperatorAppenderNotEquals());
                    put(SqlKind.GREATER_THAN, new GremlinSqlBinaryOperatorAppenderGreater());
                    put(SqlKind.GREATER_THAN_OR_EQUAL, new GremlinSqlBinaryOperatorAppenderGreaterEquals());
                    put(SqlKind.LESS_THAN, new GremlinSqlBinaryOperatorAppenderLess());
                    put(SqlKind.LESS_THAN_OR_EQUAL, new GremlinSqlBinaryOperatorAppenderLessEquals());
                    put(SqlKind.AND, new GremlinSqlBinaryOperatorAppenderAnd());
                    put(SqlKind.OR, new GremlinSqlBinaryOperatorAppenderOr());
                }
            };
    private final SqlBinaryOperator sqlBinaryOperator;
    private final SqlMetadata sqlMetadata;
    private final List<GremlinSqlNode> sqlOperands;

    public GremlinSqlBinaryOperator(final SqlBinaryOperator sqlBinaryOperator,
                                    final List<GremlinSqlNode> sqlOperands,
                                    final SqlMetadata sqlMetadata) {
        super(sqlBinaryOperator, sqlOperands, sqlMetadata);
        this.sqlBinaryOperator = sqlBinaryOperator;
        this.sqlMetadata = sqlMetadata;
        this.sqlOperands = sqlOperands;
    }

    public static void appendBooleanEquals(final SqlMetadata sqlMetadata, GraphTraversal<?, ?> graphTraversal,
                                           final GremlinSqlIdentifier identifier, boolean expectedValue)
            throws SQLException {
        GraphTraversal<?, ?> graphTraversal1 = __.unfold();
        SqlTraversalEngine.applySqlIdentifier(identifier, sqlMetadata, graphTraversal1);
        final String randomString = getRandomString();
        graphTraversal.as(randomString).where(randomString, P.eq(randomString)).
                by(graphTraversal1).by(__.unfold().constant(expectedValue));
    }

    private static String getRandomString() {
        final StringBuilder salt = new StringBuilder();
        final Random rnd = new Random();
        while (salt.length() < 10) { // length of the random string.
            final int index = (int) (rnd.nextFloat() * CHARS.length());
            salt.append(CHARS.charAt(index));
        }
        return salt.toString();
    }

    @Override
    protected void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (BINARY_APPENDERS.containsKey(sqlBinaryOperator.kind)) {
            BINARY_APPENDERS.get(sqlBinaryOperator.kind).appendTraversal(graphTraversal, sqlOperands);
        } else {
            throw new SQLException(
                    String.format("Error: Aggregate function %s is not supported.", sqlBinaryOperator.kind.sql));
        }
    }

    void handleEmbeddedGremlinSqlBasicCall(GremlinSqlBasicCall gremlinSqlBasicCall, GraphTraversal<?, ?> graphTraversal)
            throws SQLException {
        if (gremlinSqlBasicCall.getGremlinSqlNodes().size() == 1 &&
                gremlinSqlBasicCall.getGremlinSqlNodes().get(0) instanceof GremlinSqlIdentifier) {
            GremlinSqlIdentifier gremlinSqlIdentifier =
                    (GremlinSqlIdentifier) gremlinSqlBasicCall.getGremlinSqlNodes().get(0);
            if (gremlinSqlBasicCall.getGremlinSqlOperator() instanceof GremlinSqlPrefixOperator) {
                GremlinSqlPrefixOperator gremlinSqlPrefixOperator =
                        (GremlinSqlPrefixOperator) gremlinSqlBasicCall.getGremlinSqlOperator();
                if (gremlinSqlPrefixOperator.isNot()) {
                    appendBooleanEquals(sqlMetadata, graphTraversal, gremlinSqlIdentifier, false);
                } else {
                    throw new SQLException("Error: Only NOT prefix is supported in the WHERE clause.");
                }
            } else {
                appendBooleanEquals(sqlMetadata, graphTraversal, gremlinSqlIdentifier, true);
            }
        } else {
            gremlinSqlBasicCall.generateTraversal(graphTraversal);
        }
    }

    private GraphTraversal<?, ?>[] getEmbeddedLogicOperators(final List<GremlinSqlNode> operands)
            throws SQLException {
        if (operands.size() != 2) {
            throw new SQLException("Error: Binary operator without 2 operands received.");
        }
        final GraphTraversal<?, ?>[] graphTraversals = new GraphTraversal[2];
        for (int i = 0; i < operands.size(); i++) {
            graphTraversals[i] = __.__();
            if (operands.get(i) instanceof GremlinSqlIdentifier) {
                // Embedded equalities are SqlBasicCall's.
                // When the equality is struck, it is a pair of a SqlIdentifier and a SqlLiteral.
                // However, boolean columns are exceptions to this, they are just left as a SqlIdentifier
                if (!(operands.get((i == 0) ? 1 : 0) instanceof GremlinSqlLiteral)) {
                    // However, inverted logic booleans are added as SqlBasicCalls with a SqlPrefixOperator.
                    appendBooleanEquals(sqlMetadata, graphTraversals[i], (GremlinSqlIdentifier) operands.get(i), true);
                } else {
                    graphTraversals[i].values(((GremlinSqlIdentifier) operands.get(i)).getColumn());
                }
            } else if (operands.get(i) instanceof GremlinSqlBasicCall) {
                handleEmbeddedGremlinSqlBasicCall((GremlinSqlBasicCall) operands.get(i), graphTraversals[i]);
            } else if (operands.get(i) instanceof GremlinSqlLiteral) {
                ((GremlinSqlLiteral) operands.get(i)).appendTraversal(graphTraversals[i]);
            }
        }
        return graphTraversals;
    }


    private GraphTraversal<?, ?>[] getTraversalEqualities(final List<GremlinSqlNode> operands)
            throws SQLException {
        if (operands.size() != 2) {
            throw new SQLException("Error: Binary operator without 2 operands received.");
        }
        final GraphTraversal<?, ?>[] graphTraversals = new GraphTraversal[2];
        for (int i = 0; i < operands.size(); i++) {
            graphTraversals[i] = __.unfold();
            if (operands.get(i) instanceof GremlinSqlIdentifier) {
                SqlTraversalEngine.applySqlIdentifier((GremlinSqlIdentifier) operands.get(i), sqlMetadata,
                        graphTraversals[i]);
            } else if (operands.get(i) instanceof GremlinSqlBasicCall) {
                GremlinSqlBasicCall gremlinSqlBasicCall = ((GremlinSqlBasicCall) operands.get(i));
                gremlinSqlBasicCall.generateTraversal(graphTraversals[i]);
            } else if (operands.get(i) instanceof GremlinSqlLiteral) {
                ((GremlinSqlLiteral) operands.get(i)).appendTraversal(graphTraversals[i]);
            }
        }
        return graphTraversals;
    }

    public class GremlinSqlBinaryOperatorAppenderEquals implements GremlinSqlTraversalAppender {
        public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                throws SQLException {
            final String randomString = getRandomString();
            final GraphTraversal<?, ?>[] graphTraversals = getTraversalEqualities(operands);
            graphTraversal.as(randomString).where(randomString, P.eq(randomString))
                    .by(graphTraversals[0]).by(graphTraversals[1]);
        }
    }

    public class GremlinSqlBinaryOperatorAppenderNotEquals implements GremlinSqlTraversalAppender {
        public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                throws SQLException {
            final String randomString = getRandomString();
            final GraphTraversal<?, ?>[] graphTraversals = getTraversalEqualities(operands);
            graphTraversal.as(randomString).where(randomString, P.neq(randomString))
                    .by(graphTraversals[0]).by(graphTraversals[1]);
        }
    }

    public class GremlinSqlBinaryOperatorAppenderGreater implements GremlinSqlTraversalAppender {
        public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                throws SQLException {
            final String randomString = getRandomString();
            final GraphTraversal<?, ?>[] graphTraversals = getTraversalEqualities(operands);
            graphTraversal.as(randomString).where(randomString, P.gt(randomString))
                    .by(graphTraversals[0]).by(graphTraversals[1]);
        }
    }

    public class GremlinSqlBinaryOperatorAppenderGreaterEquals implements GremlinSqlTraversalAppender {
        public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                throws SQLException {
            final String randomString = getRandomString();
            final GraphTraversal<?, ?>[] graphTraversals = getTraversalEqualities(operands);
            graphTraversal.as(randomString).where(randomString, P.gte(randomString))
                    .by(graphTraversals[0]).by(graphTraversals[1]);
        }
    }

    public class GremlinSqlBinaryOperatorAppenderLess implements GremlinSqlTraversalAppender {
        public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                throws SQLException {
            final String randomString = getRandomString();
            final GraphTraversal<?, ?>[] graphTraversals = getTraversalEqualities(operands);
            graphTraversal.as(randomString).where(randomString, P.lt(randomString)).by(graphTraversals[0])
                    .by(graphTraversals[1]);
        }
    }

    public class GremlinSqlBinaryOperatorAppenderLessEquals implements GremlinSqlTraversalAppender {
        public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                throws SQLException {
            final String randomString = getRandomString();
            final GraphTraversal<?, ?>[] graphTraversals = getTraversalEqualities(operands);
            graphTraversal.as(randomString).where(randomString, P.lte(randomString)).by(graphTraversals[0])
                    .by(graphTraversals[1]);
        }
    }

    public class GremlinSqlBinaryOperatorAppenderAnd implements GremlinSqlTraversalAppender {
        public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                throws SQLException {
            graphTraversal.and(getEmbeddedLogicOperators(operands));
        }
    }

    public class GremlinSqlBinaryOperatorAppenderOr implements GremlinSqlTraversalAppender {
        public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                throws SQLException {
            graphTraversal.or(getEmbeddedLogicOperators(operands));
        }
    }

}
