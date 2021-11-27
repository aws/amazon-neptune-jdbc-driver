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

package software.aws.neptune.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.jdbc.utilities.CastHelper;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Abstract implementation of ResultSetMetaData for JDBC Driver.
 */
public abstract class ResultSetMetaData implements java.sql.ResultSetMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultSetMetaData.class);
    private final List<String> columns;

    protected ResultSetMetaData(final List<String> columns) {
        this.columns = columns;
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return CastHelper.isWrapperFor(iface, this);
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return CastHelper.unwrap(iface, LOGGER, this);
    }

    /**
     * Verify if the given column index is valid.
     *
     * @param column the 1-based column index.
     * @throws SQLException if the column index is not valid for this result.
     */
    protected void verifyColumnIndex(final int column) throws SQLException {
        if ((column <= 0) || (column > columns.size())) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.INVALID_INDEX,
                    column,
                    columns.size());
        }
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
    }

    @Override
    public int getColumnDisplaySize(final int column) throws SQLException {
        verifyColumnIndex(column);

        final int type = getColumnType(column);
        switch (type) {
            case Types.BIT:
                return 1;
            case Types.VARCHAR:
                return Integer.MAX_VALUE;
            case Types.NULL:
                return 0;
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DECIMAL:
                return 25;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return 24;
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return 20;
            default:
                LOGGER.warn(String.format("Unsupported data type for getColumnDisplaySize(%d).", type));
                return 0;
        }
    }

    @Override
    public int getPrecision(final int column) throws SQLException {
        verifyColumnIndex(column);
        final int type = getColumnType(column);
        return getTypePrecision(type);
    }

    static public int getTypePrecision(int type) {
        switch (type) {
            case Types.BOOLEAN:
                return 5;
            case Types.BIT:
                return 1;
            case Types.VARCHAR:
                return Integer.MAX_VALUE;
            case Types.NULL:
                return 0;
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DECIMAL:
                return 15;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return 24;
            case Types.BIGINT:
                return 20;
            case Types.INTEGER:
                return 11;
            case Types.SMALLINT:
                return 5;
            case Types.TINYINT:
                return 3;
            default:
                LOGGER.warn(String.format("Unsupported data type for getPrecision(%d).", type));
                return 0;
        }
    }

    @Override
    public int getScale(final int column) throws SQLException {
        verifyColumnIndex(column);

        final int columnType = getColumnType(column);
        switch (columnType) {
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.DECIMAL:
                // 15 significant digits after decimal.
                return 15;
            case Types.REAL:
                // 6 Sig significant digits after decimal.
                return 6;
            default:
                return 0;
        }
    }

    @Override
    public boolean isAutoIncrement(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Concept doesn't exist.
        return false;
    }

    @Override
    public boolean isCaseSensitive(final int column) throws SQLException {
        verifyColumnIndex(column);
        return (getColumnClassName(column).equals(String.class.getName()));
    }

    @Override
    public boolean isSearchable(final int column) throws SQLException {
        verifyColumnIndex(column);

        // We don't support WHERE clauses in the typical SQL way, so not searchable.
        return false;
    }

    @Override
    public boolean isCurrency(final int column) throws SQLException {
        verifyColumnIndex(column);

        // If it is currency, there's no way to know.
        return false;
    }

    @Override
    public int isNullable(final int column) throws SQLException {
        verifyColumnIndex(column);
        return java.sql.ResultSetMetaData.columnNullableUnknown;
    }

    @Override
    public boolean isSigned(final int column) throws SQLException {
        verifyColumnIndex(column);

        final int type = getColumnType(column);
        return ((type == Types.INTEGER) ||
                (type == Types.BIGINT) ||
                (type == Types.DOUBLE) ||
                (type == Types.FLOAT) ||
                (type == Types.REAL) ||
                (type == Types.SMALLINT) ||
                (type == Types.TINYINT) ||
                (type == Types.DECIMAL));
    }

    @Override
    public String getColumnLabel(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Label is same as name.
        return getColumnName(column);
    }

    @Override
    public String getColumnName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columns.get(column - 1);
    }

    @Override
    public boolean isReadOnly(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Read only driver.
        return true;
    }

    @Override
    public boolean isWritable(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Read only driver.
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Read only driver.
        return false;
    }

    @Override
    public String getTableName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Doesn't have the concept of tables.
        return "";
    }

    @Override
    public String getSchemaName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Doesn't have the concept of schema.
        return "";
    }

    @Override
    public String getCatalogName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Doesn't have the concept of catalog.
        return "";
    }
}
