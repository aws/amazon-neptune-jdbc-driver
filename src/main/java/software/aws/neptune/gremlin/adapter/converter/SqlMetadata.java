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

package software.aws.neptune.gremlin.adapter.converter;

import lombok.Getter;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinEdgeTable;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinProperty;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinTableBase;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinVertexTable;
import software.aws.neptune.gremlin.adapter.util.SqlGremlinError;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static software.aws.neptune.gremlin.adapter.util.SqlGremlinError.UNRECOGNIZED_TYPE;

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
    private boolean doneFilters = false;

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

    public void setIsDoneFilters(final boolean value) {
        doneFilters = value;
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
            throw SqlGremlinError.create(SqlGremlinError.EDGE_LABEL_END_MISMATCH, GremlinTableBase.IN_ID,
                    GremlinTableBase.OUT_ID);
        }

        if (gremlinTableBase.getIsVertex()) {
            throw SqlGremlinError.create(SqlGremlinError.EDGE_EXPECTED);
        }
        return gremlinTableBase.getLabel();
    }

    public boolean isLeftInRightOut(final String leftVertexLabel, final String rightVertexLabel) {
        for (final GremlinVertexTable leftVertexTable : gremlinSchema.getVertices()) {
            for (final GremlinVertexTable rightVertexTable : gremlinSchema.getVertices()) {
                if (leftVertexTable.hasInEdge(leftVertexLabel) && rightVertexTable.hasOutEdge(rightVertexLabel)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isRightInLeftOut(final String leftVertexLabel, final String rightVertexLabel) {
        for (final GremlinVertexTable leftVertexTable : gremlinSchema.getVertices()) {
            for (final GremlinVertexTable rightVertexTable : gremlinSchema.getVertices()) {
                if (leftVertexTable.hasOutEdge(leftVertexLabel) && rightVertexTable.hasInEdge(rightVertexLabel)) {
                    return true;
                }
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
        throw SqlGremlinError.create(SqlGremlinError.TABLE_DOES_NOT_EXIST, renamedTableName);
    }

    public GremlinTableBase getGremlinTable(final String table) throws SQLException {
        final String renamedTableName = getRenamedTable(table);
        for (final GremlinTableBase gremlinTableBase : gremlinSchema.getAllTables()) {
            if (gremlinTableBase.getLabel().equalsIgnoreCase(renamedTableName)) {
                return gremlinTableBase;
            }
        }
        throw SqlGremlinError.create(SqlGremlinError.TABLE_DOES_NOT_EXIST, renamedTableName);
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
        if (table.hasColumn(column)) {
            return table.getColumn(column).getName();
        } else if (columnRenameMap.containsKey(column)) {
            return table.getColumn(getRenamedColumn(column)).getName();
        }
        final Optional<String> actualName = columnRenameMap.entrySet().stream().
                filter(entry -> entry.getValue().equals(column)).map(Map.Entry::getKey).findFirst();
        return table.getColumn(actualName.orElse(column)).getName();
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
        throw SqlGremlinError.create(SqlGremlinError.ERROR_TABLE, table);
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

    public String getType(final String column) throws SQLException {
        final List<GremlinTableBase> gremlinTableBases = new ArrayList<>();
        for (final String table : getColumnOutputListMap().keySet()) {
            gremlinTableBases.add(getGremlinTable(table));
        }
        if (aggregateTypeExists(column)) {
            return getOutputType(column, "string");
        }
        String renamedColumn = getRenamedColumn(column);
        if (!aggregateTypeExists(renamedColumn)) {
            // Sometimes columns are double renamed.
            renamedColumn = getRenamedColumn(renamedColumn);
            for (final GremlinTableBase gremlinTableBase : gremlinTableBases) {
                if (getTableHasColumn(gremlinTableBase, renamedColumn)) {
                    return getGremlinProperty(gremlinTableBase.getLabel(), renamedColumn).getType();
                }
            }
        }
        return getOutputType(renamedColumn, "string");
    }

    public Object getDefaultCoalesceValue(final String column) throws SQLException {
        final String type = getType(column);
        switch (getType(column)) {
            case "string":
                return "";
            case "boolean":
                return false;
            case "byte":
                return Byte.MAX_VALUE;
            case "short":
                return Short.MAX_VALUE;
            case "integer":
                return Integer.MAX_VALUE;
            case "long":
                return Long.MAX_VALUE;
            case "float":
                return Float.MAX_VALUE;
            case "double":
                return Double.MAX_VALUE;
            case "date":
                return Date.from(Instant.EPOCH);
        }
        throw SqlGremlinError.create(UNRECOGNIZED_TYPE, type);
    }
}
