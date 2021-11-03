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

package software.aws.neptune.gremlin.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.results.SqlGremlinQueryResult;
import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetColumns;
import software.aws.neptune.gremlin.GremlinTypeMapping;
import software.aws.neptune.gremlin.resultset.GremlinResultSet;
import software.aws.neptune.gremlin.resultset.GremlinResultSetMetadata;
import software.aws.neptune.jdbc.ResultSet;
import software.aws.neptune.jdbc.utilities.SqlError;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SqlGremlinResultSet extends ResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinResultSet.class);

    private final List<String> columns;
    private final List<String> columnTypes;
    private final GremlinResultSetMetadata gremlinResultSetMetadata;
    private final SqlGremlinQueryResult sqlQueryResult;
    // A single row that's assigned when we use getResult() in next().
    private List<Object> row;
    private boolean wasNull = false;

    /**
     * GremlinResultSet constructor, initializes super class.
     *
     * @param statement   Statement Object.
     * @param queryResult SqlGremlinQueryResult Object.
     */
    public SqlGremlinResultSet(final java.sql.Statement statement,
                               final SqlGremlinQueryResult queryResult) {
        // 1 for row count as placeholder.
        super(statement, queryResult.getColumns(), 1);
        this.columns = queryResult.getColumns();
        // Null until we get result by calling next.
        this.row = null;
        this.columnTypes = queryResult.getColumnTypes();
        this.sqlQueryResult = queryResult;

        final List<Class<?>> rowTypes = new ArrayList<>();
        for (final String columnType : columnTypes) {
            final Optional<? extends Class<?>> javaClassOptional =
                    ResultSetGetColumns.GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.
                            entrySet().stream().
                            filter(d -> d.getKey().equalsIgnoreCase(columnType)).
                            map(Map.Entry::getValue).
                            findFirst();
            rowTypes.add(javaClassOptional.isPresent() ? javaClassOptional.get() : String.class);
        }
        gremlinResultSetMetadata = new GremlinResultSetMetadata(columns, rowTypes);
    }

    @Override
    protected void doClose() throws SQLException {
    }

    @Override
    public boolean next() throws SQLException {
        final Object res;
        try {
            res = sqlQueryResult.getResult();
        } catch (final SQLException e) {
            if (e.getMessage().equals(SqlGremlinQueryResult.EMPTY_MESSAGE)) {
                LOGGER.trace(SqlGremlinQueryResult.EMPTY_MESSAGE);
                return false;
            } else {
                throw e;
            }
        }
        this.row = (List<Object>) res;
        return true;
    }

    @Override
    public boolean isLast() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public boolean absolute(final int row) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    // TODO use fetch size for page size?
    protected int getDriverFetchSize() throws SQLException {
        return 0;
    }

    @Override
    // TODO use fetch size for page size?
    protected void setDriverFetchSize(final int rows) {
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

        // Grab value in row using column index (note: 1 based indexing of JDBC hence -1).
        final Object value = row.get(columnIndex - 1);
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
