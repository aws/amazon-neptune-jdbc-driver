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
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
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

    public GremlinSqlPrefixOperator(final SqlPrefixOperator sqlPrefixOperator, final List<GremlinSqlNode> gremlinSqlNodes,
                                    final SqlMetadata sqlMetadata) {
        super(sqlPrefixOperator, gremlinSqlNodes, sqlMetadata);
        this.sqlPrefixOperator = sqlPrefixOperator;
        this.sqlMetadata = sqlMetadata;
        this.sqlOperands = gremlinSqlNodes;
    }

    public boolean isNot() {
        return sqlPrefixOperator.kind.equals(SqlKind.NOT);
    }

    @Override
    protected void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
    }

}
