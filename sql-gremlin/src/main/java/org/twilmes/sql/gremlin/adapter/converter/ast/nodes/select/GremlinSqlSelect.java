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

package org.twilmes.sql.gremlin.adapter.converter.ast.nodes.select;

import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.SqlTraversalEngine;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlBasicCall;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinTableBase;
import org.twilmes.sql.gremlin.adapter.results.SqlGremlinQueryResult;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This abstract class is a GremlinSql equivalent of Calcite's SqlSelect.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public abstract class GremlinSqlSelect extends GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlSelect.class);
    private final GraphTraversalSource g;
    private final SqlSelect sqlSelect;
    private final SqlMetadata sqlMetadata;

    public GremlinSqlSelect(final SqlSelect sqlSelect, final SqlMetadata sqlMetadata, final GraphTraversalSource g) {
        super(sqlSelect, sqlMetadata);
        this.sqlSelect = sqlSelect;
        this.g = g;
        this.sqlMetadata = sqlMetadata;
    }

    public SqlGremlinQueryResult executeTraversal() throws SQLException {
        GraphTraversal<?, ?> graphTraversal = null;
        try {
            sqlMetadata.checkAggregate(sqlSelect.getSelectList());
            sqlMetadata.checkGroupByNodeIsNull(sqlSelect.getGroup());
            graphTraversal = generateTraversal();
            applyDistinct(graphTraversal);
            applyOffset(graphTraversal);
            applyLimit(graphTraversal);
            final SqlGremlinQueryResult sqlGremlinQueryResult = generateSqlGremlinQueryResult();
            runTraversalExecutor(graphTraversal, sqlGremlinQueryResult);
            return sqlGremlinQueryResult;
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

    private SqlGremlinQueryResult generateSqlGremlinQueryResult() throws SQLException {
        final List<GremlinTableBase> tables = new ArrayList<>();
        final List<String> columns = new ArrayList<>();
        for (final String table : sqlMetadata.getColumnOutputListMap().keySet()) {
            tables.add(sqlMetadata.getGremlinTable(table));
        }
        sqlMetadata.getColumnOutputListMap().forEach((key, value) -> columns.addAll(value));
        return new SqlGremlinQueryResult(columns, tables, sqlMetadata);

    }

    protected abstract void runTraversalExecutor(GraphTraversal<?, ?> traversal,
                                                 SqlGremlinQueryResult sqlGremlinQueryResult) throws SQLException;

    public String getStringTraversal() throws SQLException {
        return GroovyTranslator.of("g").translate(generateTraversal().asAdmin().getBytecode());
    }

    public abstract GraphTraversal<?, ?> generateTraversal() throws SQLException;

    protected GraphTraversal<?, ?> applyColumnRenames(final List<GremlinSqlNode> sqlNodeList, final String table) throws SQLException {
        // Determine what the names should be for renaming.
        final List<String> columnsRenamed = new ArrayList<>();
        for (final GremlinSqlNode gremlinSqlNode : sqlNodeList) {
            if (gremlinSqlNode instanceof GremlinSqlIdentifier) {
                columnsRenamed.add(((GremlinSqlIdentifier) gremlinSqlNode).getName(1));
            } else if (gremlinSqlNode instanceof GremlinSqlBasicCall) {
                columnsRenamed.add(((GremlinSqlBasicCall) gremlinSqlNode).getRename());
            } else {
                throw new SQLException(String.format(
                        "Error: Unknown sql node type for select list %s.", gremlinSqlNode.getClass().getName()));
            }
        }

        final List<String> renamedColumnsTemp = new ArrayList<>(columnsRenamed);
        sqlMetadata.setColumnOutputList(table, columnsRenamed);
        return SqlTraversalEngine.applyColumnRenames(renamedColumnsTemp);
    }

    protected void applyColumnRetrieval(final GraphTraversal<?, ?> graphTraversal, final String table,
                                        final List<GremlinSqlNode> sqlNodeList, final StepDirection stepDirection)
            throws SQLException {
        // If there are no nodes, we should simply append a by and exit.
        if (sqlNodeList.isEmpty()) {
            graphTraversal.by();
            return;
        }

        final GraphTraversal<?, ?> subGraphTraversal = applyColumnRenames(sqlNodeList, table);
        for (final GremlinSqlNode gremlinSqlNode : sqlNodeList) {
            if (gremlinSqlNode instanceof GremlinSqlIdentifier) {
                final GraphTraversal<?, ?> subSubGraphTraversal =
                        SqlTraversalEngine.getEmptyTraversal(stepDirection, sqlMetadata);
                SqlTraversalEngine
                        .applySqlIdentifier((GremlinSqlIdentifier) gremlinSqlNode, sqlMetadata, subSubGraphTraversal);
                SqlTraversalEngine.applyTraversal(subGraphTraversal, subSubGraphTraversal);
            } else if (gremlinSqlNode instanceof GremlinSqlBasicCall) {
                final GraphTraversal<?, ?> subSubGraphTraversal =
                        SqlTraversalEngine.getEmptyTraversal(stepDirection, sqlMetadata);
                ((GremlinSqlBasicCall) gremlinSqlNode).generateTraversal(subSubGraphTraversal);
                SqlTraversalEngine.applyTraversal(subGraphTraversal, subSubGraphTraversal);
            } else {
                throw new SQLException(String.format(
                        "Error: Unknown sql node type for select list %s.", gremlinSqlNode.getClass().getName()));
            }
        }
        SqlTraversalEngine.applyTraversal(graphTraversal, subGraphTraversal);
    }

    protected void applyColumnRetrieval(final GraphTraversal<?, ?> graphTraversal, final String table,
                                        final List<GremlinSqlNode> sqlNodeList) throws SQLException {
        applyColumnRetrieval(graphTraversal, table, sqlNodeList, StepDirection.None);
    }

    private void applyOffset(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        // TODO: AN-885 implement OFFSET
        // Gremlin doesn't seem to directly support offset,
        // we probably need to inject numeric literal value
        // into the pagination and have it know to jump the
        // first X number of results.
        if (sqlSelect.getOffset() != null) {
            throw new SQLException("Error, OFFSET is not currently supported.");
        }
    }

    private void applyLimit(final GraphTraversal<?, ?> graphTraversal) {
        if (sqlSelect.getFetch() instanceof SqlNumericLiteral) {
            final SqlNumericLiteral limit = (SqlNumericLiteral) sqlSelect.getFetch();
            final Long limitValue = limit.getValueAs(Long.class);
            graphTraversal.limit(limitValue);
        }
    }

    private void applyDistinct(final GraphTraversal<?, ?> graphTraversal) {
        if (sqlSelect.isDistinct()) {
            graphTraversal.dedup();
        }
    }
}
