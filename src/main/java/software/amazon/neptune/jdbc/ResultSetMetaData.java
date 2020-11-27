/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.neptune.jdbc;

import java.sql.SQLException;

/**
 * Abstract implementation of java.sql.ResultSetMetaData for Neptune JDBC Driver.
 * Concrete implementations will be provided in query language specific implementations.
 */
public abstract class ResultSetMetaData implements java.sql.ResultSetMetaData {
    @Override
    public int getColumnCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean isAutoIncrement(final int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(final int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(final int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(final int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(final int column) throws SQLException {
        return 0;
    }

    @Override
    public boolean isSigned(final int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(final int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnLabel(final int column) throws SQLException {
        return null;
    }

    @Override
    public String getColumnName(final int column) throws SQLException {
        return null;
    }

    @Override
    public String getSchemaName(final int column) throws SQLException {
        return null;
    }

    @Override
    public int getPrecision(final int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(final int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(final int column) throws SQLException {
        return null;
    }

    @Override
    public String getCatalogName(final int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(final int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnTypeName(final int column) throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly(final int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(final int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(final int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(final int column) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return false;
    }
}
