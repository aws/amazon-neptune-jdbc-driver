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

import lombok.Getter;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlFactory;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.aggregate.GremlinSqlAggFunction;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.logic.GremlinSqlBinaryOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.logic.GremlinSqlLiteral;
import org.twilmes.sql.gremlin.adapter.util.SqlGremlinError;
import java.sql.SQLException;
import java.util.List;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlBasicCall.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
@Getter
public class GremlinSqlBasicCall extends GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlBasicCall.class);
    private final SqlBasicCall sqlBasicCall;
    private final GremlinSqlOperator gremlinSqlOperator;
    private final List<GremlinSqlNode> gremlinSqlNodes;

    public GremlinSqlBasicCall(final SqlBasicCall sqlBasicCall, final SqlMetadata sqlMetadata)
            throws SQLException {
        super(sqlBasicCall, sqlMetadata);
        this.sqlBasicCall = sqlBasicCall;
        gremlinSqlOperator =
                GremlinSqlFactory.createOperator(sqlBasicCall.getOperator(), sqlBasicCall.getOperandList());
        gremlinSqlNodes = GremlinSqlFactory.createNodeList(sqlBasicCall.getOperandList());
    }

    void validate() throws SQLException {
        if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            if (gremlinSqlNodes.size() != 2) {
                throw SqlGremlinError.create(SqlGremlinError.UNEXPECTED_NODE_GREMLINSQLBASICCALL);
            }
        } else if (gremlinSqlOperator instanceof GremlinSqlAggFunction) {
            if (gremlinSqlNodes.size() != 1) {
                throw SqlGremlinError.create(SqlGremlinError.UNEXPECTED_NODE_GREMLINSQLAGGFUNCTION);
            }
        }
    }

    public void generateTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        validate();
        gremlinSqlOperator.appendOperatorTraversal(graphTraversal);
    }

    public String getRename() throws SQLException {
        if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            return ((GremlinSqlAsOperator) gremlinSqlOperator).getRename();
        } else if (gremlinSqlOperator instanceof GremlinSqlAggFunction) {
            if (gremlinSqlNodes.size() == 1 &&
                    (gremlinSqlNodes.get(0) instanceof GremlinSqlIdentifier ||
                            gremlinSqlNodes.get(0) instanceof GremlinSqlLiteral)) {
                // returns the formatted column name for aggregations
                return ((GremlinSqlAggFunction) gremlinSqlOperator).getNewName();
            }
        } else if (gremlinSqlOperator instanceof GremlinSqlBinaryOperator) {
            return ((GremlinSqlBinaryOperator) gremlinSqlOperator).getNewName();
        } else if (gremlinSqlOperator instanceof GremlinSqlPrefixOperator) {
            return ((GremlinSqlPrefixOperator) gremlinSqlOperator).getNewName();
        } else if (gremlinSqlOperator instanceof GremlinSqlPostfixOperator) {
            return ((GremlinSqlPostfixOperator) gremlinSqlOperator).getNewName();
        }
        throw SqlGremlinError.create(SqlGremlinError.COLUMN_RENAME_UNDETERMINED);
    }

    public String getActual() throws SQLException {
        if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            return ((GremlinSqlAsOperator) gremlinSqlOperator).getActual();
        } else if (gremlinSqlOperator instanceof GremlinSqlAggFunction) {
            return ((GremlinSqlAggFunction) gremlinSqlOperator).getNewName();
        } else if (gremlinSqlOperator instanceof GremlinSqlBinaryOperator) {
            return ((GremlinSqlBinaryOperator) gremlinSqlOperator).getNewName();
        } else if (gremlinSqlOperator instanceof GremlinSqlPrefixOperator) {
            return ((GremlinSqlPrefixOperator) gremlinSqlOperator).getNewName();
        } else if (gremlinSqlOperator instanceof GremlinSqlPostfixOperator) {
            return ((GremlinSqlPostfixOperator) gremlinSqlOperator).getNewName();
        }
        throw SqlGremlinError.create(SqlGremlinError.COLUMN_ACTUAL_NAME_UNDETERMINED);
    }

    public String getOutputColumn() throws SQLException {
        if (gremlinSqlNodes.size() != 1) {
            throw SqlGremlinError.create(SqlGremlinError.COLUMN_ACTUAL_NAME_UNDETERMINED);
        }
        if (gremlinSqlNodes.get(0) instanceof GremlinSqlIdentifier) {
            return ((GremlinSqlIdentifier) gremlinSqlNodes.get(0)).getColumn();
        } else if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            return ((GremlinSqlAsOperator) gremlinSqlOperator).getActual();
        }
        throw SqlGremlinError.create(SqlGremlinError.COLUMN_ACTUAL_NAME_UNDETERMINED);
    }
}
