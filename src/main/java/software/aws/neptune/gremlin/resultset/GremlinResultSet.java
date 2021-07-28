/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.gremlin.resultset;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.common.ResultSetInfoWithoutRows;
import software.aws.neptune.gremlin.GremlinTypeMapping;
import software.aws.neptune.jdbc.ResultSet;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Gremlin ResultSet class.
 */
public class GremlinResultSet extends ResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinResultSet.class);
    private final List<String> columns;
    private final List<Map<String, Object>> rows;
    private final Map<String, Class<?>> columnTypes;
    private boolean wasNull = false;

    /**
     * GremlinResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfo ResultSetInfoWithRows Object.
     */
    public GremlinResultSet(final java.sql.Statement statement, final ResultSetInfoWithRows resultSetInfo) {
        super(statement, resultSetInfo.getColumns(), resultSetInfo.getRows().size());
        this.columns = resultSetInfo.getColumns();
        this.rows = resultSetInfo.getRows();
        this.columnTypes = resultSetInfo.getColumnsTypes();
    }

    /**
     * GremlinResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfo ResultSetInfoWithoutRows Object.
     */
    public GremlinResultSet(final java.sql.Statement statement, final ResultSetInfoWithoutRows resultSetInfo) {
        super(statement, resultSetInfo.getColumns(), resultSetInfo.getRowCount());
        this.columns = resultSetInfo.getColumns();
        this.columnTypes = new HashMap<>();
        this.rows = null;
    }

    @Override
    protected void doClose() throws SQLException {
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        final List<Class<?>> rowTypes = new ArrayList<>();
        for (final String column : columns) {
            rowTypes.add(columnTypes.getOrDefault(column, String.class));
        }
        return new GremlinResultSetMetadata(columns, rowTypes);
    }

    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        final Object value = getValue(columnIndex);
        return (value == null) || GremlinTypeMapping.checkContains(value.getClass())
                ? value
                : value.toString();
    }

    private Object getValue(final int columnIndex) throws SQLException {
        verifyOpen();
        if (rows == null) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.UNSUPPORTED_RESULT_SET_TYPE);
        }
        validateRowColumn(columnIndex);

        final String colName = columns.get(columnIndex - 1);
        final Map<String, Object> row = rows.get(getRowIndex());
        final Object value = row.getOrDefault(colName, null);
        wasNull = (value == null);

        return value;
    }

    @Override
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
        LOGGER.trace("Getting column {} as an Object using provided Map.", columnIndex);
        final Object value = getValue(columnIndex);
        return getObject(columnIndex, map.get(GremlinTypeMapping.getJDBCType(value.getClass()).name()));
    }

    @AllArgsConstructor
    @Getter
    public static class ResultSetInfoWithRows {
        private final List<Map<String, Object>> rows;
        private final Map<String, Class<?>> columnsTypes;
        private final List<String> columns;
    }
}
