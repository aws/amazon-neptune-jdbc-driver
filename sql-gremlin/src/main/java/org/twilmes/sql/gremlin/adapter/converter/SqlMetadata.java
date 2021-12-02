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

package org.twilmes.sql.gremlin.adapter.converter;

import lombok.Getter;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinEdgeTable;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinProperty;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinTableBase;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinVertexTable;
import org.twilmes.sql.gremlin.adapter.util.SqlGremlinError;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This module contains traversal and query metadata used by the adapter.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
@Getter
public class SqlMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlMetadata.class);
    private final GremlinSchema gremlinSchema;
    private final Map<String, String> tableRenameMap = new HashMap<>();
    private final Map<String, String> columnRenameMap = new HashMap<>();
    private final Map<String, List<String>> columnOutputListMap = new HashMap<>();
    // maps the aggregated columns to type
    private final Map<String, String> aggregateTypeMap = new HashMap<>();
    private boolean isAggregate = false;
    private boolean isGrouped = false;

    public SqlMetadata(final GremlinSchema gremlinSchema) {
        this.gremlinSchema = gremlinSchema;
    }

    private static boolean isAggregate(final SqlNode sqlNode) {
        if (sqlNode instanceof SqlCall) {
            final SqlCall sqlCall = (SqlCall) sqlNode;
            if (isAggregate(sqlCall.getOperator())) {
                return true;
            }
            for (final SqlNode tmpSqlNode : sqlCall.getOperandList()) {
                if (isAggregate(tmpSqlNode)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isAggregate(final SqlOperator sqlOperator) {
        return sqlOperator instanceof SqlAggFunction;
    }

    public boolean getIsProjectFoldRequired() {
        // Grouping invokes an implicit fold before the project that does not require additional unfolding.
        // Folding is required for aggregates that are not grouped.
        return getIsAggregate() && !getIsGrouped();
    }

    public boolean getIsGrouped() {
        return isGrouped;
    }

    public boolean getIsColumnEdge(final String tableName, final String columnName) throws SQLException {
        return getGremlinTable(tableName).getIsVertex() &&
                (columnName.endsWith(GremlinTableBase.IN_ID) || columnName.endsWith(GremlinTableBase.OUT_ID));
    }

    public String getColumnEdgeLabel(final String column) throws SQLException {
        final String columnName = getRenamedColumn(column);
        final GremlinTableBase gremlinTableBase;
        if (columnName.endsWith(GremlinTableBase.IN_ID)) {
            gremlinTableBase = getGremlinTable(column.substring(0, column.length() - GremlinTableBase.IN_ID.length()));
        } else if (columnName.endsWith(GremlinTableBase.OUT_ID)) {
            gremlinTableBase = getGremlinTable(column.substring(0, column.length() - GremlinTableBase.OUT_ID.length()));
        } else {
            throw SqlGremlinError.get(SqlGremlinError.EDGE_LABEL_END_MISMATCH, GremlinTableBase.IN_ID,
                    GremlinTableBase.OUT_ID);
        }

        if (gremlinTableBase.getIsVertex()) {
            throw SqlGremlinError.get(SqlGremlinError.EDGE_EXPECTED);
        }
        return gremlinTableBase.getLabel();
    }

    public boolean isLeftInRightOut(final String leftVertexLabel, final String rightVertexLabel) {
        for (final GremlinVertexTable gremlinVertexTable : gremlinSchema.getVertices()) {
            if (gremlinVertexTable.hasInEdge(leftVertexLabel) && gremlinVertexTable.hasOutEdge(rightVertexLabel)) {
                return true;
            }
        }
        return false;
    }

    public boolean isRightInLeftOut(final String leftVertexLabel, final String rightVertexLabel) {
        for (final GremlinVertexTable gremlinVertexTable : gremlinSchema.getVertices()) {
            if (gremlinVertexTable.hasInEdge(rightVertexLabel) && gremlinVertexTable.hasOutEdge(leftVertexLabel)) {
                return true;
            }
        }
        return false;
    }

    public Set<String> getRenamedColumns() {
        return new HashSet<>(columnRenameMap.keySet());
    }

    public void setColumnOutputList(final String table, final List<String> columnOutputList) {
        columnOutputListMap.put(table, new ArrayList<>(columnOutputList));
    }

    public Set<GremlinTableBase> getTables() throws SQLException {
        final Set<GremlinTableBase> tables = new HashSet<>();
        for (final String table : tableRenameMap.values()) {
            tables.add(getGremlinTable(table));
        }
        return tables;
    }

    public boolean isVertex(final String table) throws SQLException {
        final String renamedTableName = getRenamedTable(table);
        for (final GremlinVertexTable gremlinVertexTable : gremlinSchema.getVertices()) {
            if (gremlinVertexTable.getLabel().equalsIgnoreCase(renamedTableName)) {
                return true;
            }
        }
        for (final GremlinEdgeTable gremlinEdgeTable : gremlinSchema.getEdges()) {
            if (gremlinEdgeTable.getLabel().equalsIgnoreCase(renamedTableName)) {
                return false;
            }
        }
        throw SqlGremlinError.get(SqlGremlinError.TABLE_DOES_NOT_EXIST, renamedTableName);
    }

    public GremlinTableBase getGremlinTable(final String table) throws SQLException {
        final String renamedTableName = getRenamedTable(table);
        for (final GremlinTableBase gremlinTableBase : gremlinSchema.getAllTables()) {
            if (gremlinTableBase.getLabel().equalsIgnoreCase(renamedTableName)) {
                return gremlinTableBase;
            }
        }
        throw SqlGremlinError.get(SqlGremlinError.TABLE_DOES_NOT_EXIST, renamedTableName);
    }

    public void addRenamedTable(final String actualName, final String renameName) {
        tableRenameMap.put(renameName, actualName);
    }

    public String getRenamedTable(final String table) {
        return tableRenameMap.getOrDefault(table, table);
    }

    public void addRenamedColumn(final String actualName, final String renameName) {
        columnRenameMap.put(renameName, actualName);
    }

    public String getRenamedColumn(final String column) {
        return columnRenameMap.getOrDefault(column, column);
    }

    public boolean aggregateTypeExists(final String column) {
        return aggregateTypeMap.containsKey(column);
    }

    public String getRenameFromActual(final String actual) {
        final Optional<Map.Entry<String, String>>
                rename = tableRenameMap.entrySet().stream().filter(t -> t.getValue().equals(actual)).findFirst();
        if (rename.isPresent()) {
            return rename.get().getKey();
        }
        return actual;
    }

    public String getActualColumnName(final GremlinTableBase table, final String column) throws SQLException {
        final String actualColumnName = getRenamedColumn(column);
        return table.getColumn(actualColumnName).getName();
    }

    public boolean getTableHasColumn(final GremlinTableBase table, final String column) {
        final String actualColumnName = getRenamedColumn(column);
        return table.hasColumn(actualColumnName);
    }

    public String getActualTableName(final String table) throws SQLException {
        final String renamedTableName = getRenamedTable(table);
        for (final GremlinVertexTable gremlinVertexTable : gremlinSchema.getVertices()) {
            if (gremlinVertexTable.getLabel().equalsIgnoreCase(renamedTableName)) {
                return gremlinVertexTable.getLabel();
            }
        }
        for (final GremlinEdgeTable gremlinEdgeTable : gremlinSchema.getEdges()) {
            if (gremlinEdgeTable.getLabel().equalsIgnoreCase(renamedTableName)) {
                return gremlinEdgeTable.getLabel();
            }
        }
        throw SqlGremlinError.get(SqlGremlinError.ERROR_TABLE, table);
    }

    public void checkAggregate(final SqlNodeList sqlNodeList) {
        isAggregate = sqlNodeList.getList().stream().anyMatch(SqlMetadata::isAggregate);
    }

    public void checkGroupByNodeIsNull(final SqlNode sqlNode) {
        isGrouped = sqlNode != null;
    }

    public boolean getIsAggregate() {
        return isAggregate;
    }

    public GremlinProperty getGremlinProperty(final String table, final String column) throws SQLException {
        final String actualColumnName = getActualColumnName(getGremlinTable(table), column);
        return getGremlinTable(table).getColumn(actualColumnName);
    }

    public void addOutputType(final String outputName, final String colType) {
        aggregateTypeMap.put(outputName, colType);
    }

    public String getOutputType(final String outputName, final String colType) {
        return aggregateTypeMap.getOrDefault(outputName, colType);
    }
}
