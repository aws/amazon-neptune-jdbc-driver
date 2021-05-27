/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.neptune.gremlin.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.processor.SingleQueryExecutor;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import software.amazon.neptune.gremlin.GremlinTypeMapping;
import software.amazon.neptune.gremlin.resultset.GremlinResultSet;
import software.amazon.neptune.gremlin.resultset.GremlinResultSetMetadata;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static software.amazon.neptune.gremlin.GremlinTypeMapping.GREMLIN_TO_JDBC_TYPE_MAP;

public class SqlGremlinResultSet extends software.amazon.jdbc.ResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinResultSet.class);
    private static final Map<String, Class<?>> SQL_GREMLIN_COLUMN_TYPE_TO_JAVA_TYPE = new HashMap<>();

    static {
        GREMLIN_TO_JDBC_TYPE_MAP.keySet().forEach(gremlinType -> {
            SQL_GREMLIN_COLUMN_TYPE_TO_JAVA_TYPE.put(gremlinType.getName().toLowerCase(), gremlinType);
        });
    }

    private final List<String> columns;
    private final List<List<Object>> rows;
    private final List<String> columnTypes;
    private boolean wasNull = false;
    private final GremlinResultSetMetadata gremlinResultSetMetadata;

    /**
     * GremlinResultSet constructor, initializes super class.
     *
     * @param statement   Statement Object.
     * @param queryResult SqlGremlinQueryResult Object.
     */
    public SqlGremlinResultSet(final java.sql.Statement statement,
                               final SingleQueryExecutor.SqlGremlinQueryResult queryResult) {
        super(statement, queryResult.getColumns(), queryResult.getRows().size());
        this.columns = queryResult.getColumns();
        this.rows = queryResult.getRows();
        this.columnTypes = queryResult.getColumnTypes();

        final List<Class<?>> rowTypes = new ArrayList<>();
        for (final String columnType : columnTypes) {
            rowTypes.add(SQL_GREMLIN_COLUMN_TYPE_TO_JAVA_TYPE.getOrDefault(columnType.toLowerCase(), String.class));
        }
        gremlinResultSetMetadata = new GremlinResultSetMetadata(columns, rowTypes);
    }

    @Override
    protected void doClose() throws SQLException {
    }

    @Override
    protected int getDriverFetchSize() throws SQLException {
        // Can't be done based on implementation.
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {
        // Can't be done based on implementation.
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        return gremlinResultSetMetadata;
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

        // Look for row index within rows, then grab column index from there (note: 1 based indexing of JDBC hence -1).
        final Object value = rows.get(getRowIndex()).get(columnIndex - 1);
        wasNull = (value == null);

        return value;
    }

    @Override
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
        LOGGER.trace("Getting column {} as an Object using provided Map.", columnIndex);
        final Object value = getValue(columnIndex);
        return getObject(columnIndex, map.get(GremlinTypeMapping.getJDBCType(value.getClass()).name()));
    }
}
