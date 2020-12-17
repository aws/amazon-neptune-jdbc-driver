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

package software.amazon.neptune.opencypher;


import lombok.AllArgsConstructor;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.ResultSetMetaData;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;

import java.sql.SQLException;
import java.util.List;

/**
 * OpenCypher implementation of ResultSetMetadata.
 */
@AllArgsConstructor
public class OpenCypherResultSetMetadata extends ResultSetMetaData implements java.sql.ResultSetMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherResultSetMetadata.class);
    private final Result result;
    private final List<String> columns;
    private final List<Record> rows;

    /**
     * Verify if the given column index is valid.
     *
     * @param column the 1-based column index.
     * @throws SQLException if the column index is not valid for this result.
     */
    private void verifyColumnIndex(final int column) throws SQLException {
        if ((column < 0) || (column >= columns.size())) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.INVALID_INDEX,
                    column,
                    columns.size());
        }
    }

    private Value getColumnValue(final int column) {
        // TODO: loop through the result set and obtain the common type for all values in the same column.
        return rows.get(0).values().get(column - 1);
    }

    // TODO: All below are just default stub implementations.
    @Override
    public int getColumnCount() throws SQLException {
        // TODO: Update strings to use resource file.
        if (columns == null) {
            throw new SQLException("Failed to get valid columns from database.");
        }
        return columns.size();
    }

    @Override
    public boolean isAutoIncrement(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return false;
    }

    @Override
    public boolean isCaseSensitive(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return false;
    }

    @Override
    public boolean isSearchable(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return false;
    }

    @Override
    public boolean isCurrency(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return false;
    }

    @Override
    public int isNullable(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return 0;
    }

    @Override
    public boolean isSigned(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return false;
    }

    @Override
    public int getColumnDisplaySize(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return 0;
    }

    @Override
    public String getColumnLabel(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return null;
    }

    @Override
    public String getColumnName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return null;
    }

    @Override
    public String getSchemaName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return null;
    }

    @Override
    public int getPrecision(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return 0;
    }

    @Override
    public int getScale(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return 0;
    }

    @Override
    public String getTableName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Doesn't have the concept of tables.
        return "";
    }

    @Override
    public String getCatalogName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Doesn't have the concept of tables.
        return "";
    }

    @Override
    public int getColumnType(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO - return SQL type, mapped from Neo4j database type
        // getColumnValue(column).type() -> int
        return 0;
    }

    @Override
    public String getColumnTypeName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return getColumnValue(column).type().name();
    }

    @Override
    public boolean isReadOnly(final int column) throws SQLException {
        verifyColumnIndex(column);

        return true;
    }

    @Override
    public boolean isWritable(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return false;
    }

    @Override
    public String getColumnClassName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // TODO
        return null;
    }
}
