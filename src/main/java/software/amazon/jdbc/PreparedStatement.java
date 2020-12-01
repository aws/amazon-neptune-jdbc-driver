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

package software.amazon.jdbc;

import software.amazon.jdbc.utilities.SqlError;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Abstract implementation of PreparedStatement for JDBC Driver.
 */
public abstract class PreparedStatement extends software.amazon.jdbc.Statement implements java.sql.PreparedStatement {
    private final String sql;
    private ResultSet resultSet;

    /**
     * Constructor for seeding the prepared statement with the parent connection.
     *
     * @param connection The parent connection.
     * @param sql        The sql query.
     * @throws SQLException if error occurs when get type map of connection.
     */
    protected PreparedStatement(final Connection connection, final String sql) {
        super(connection);
        this.sql = sql;
    }

    @Override
    public void addBatch() throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute() throws SQLException {
        this.resultSet = executeQuery();
        return true;
    }

    @Override
    public boolean execute(final String sql) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public ResultSet executeQuery(final String sql) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void clearParameters() throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public int executeUpdate() throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.READ_ONLY));
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setArray(final int parameterIndex, final Array x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final int length)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final long length)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final int length)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final long length)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setBlob(final int parameterIndex, final Blob x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(
                SqlError.lookup(SqlError.UNSUPPORTED_TYPE, Blob.class.toString()));
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream inputStream, final long length)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(
                SqlError.lookup(SqlError.UNSUPPORTED_TYPE, Blob.class.toString()));
    }

    @Override
    public void setBlob(final int parameterIndex, final InputStream inputStream) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(
                SqlError.lookup(SqlError.UNSUPPORTED_TYPE, Blob.class.toString()));
    }

    @Override
    public void setBoolean(final int parameterIndex, final boolean x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setByte(final int parameterIndex, final byte x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setBytes(final int parameterIndex, final byte[] x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final int length)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader, final long length)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setClob(final int parameterIndex, final Clob x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(
                SqlError.lookup(SqlError.UNSUPPORTED_TYPE, Clob.class.toString()));
    }

    @Override
    public void setClob(final int parameterIndex, final Reader reader, final long length)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(
                SqlError.lookup(SqlError.UNSUPPORTED_TYPE, Clob.class.toString()));
    }

    @Override
    public void setClob(final int parameterIndex, final Reader reader) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(
                SqlError.lookup(SqlError.UNSUPPORTED_TYPE, Clob.class.toString()));
    }

    @Override
    public void setDate(final int parameterIndex, final Date x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setDate(final int parameterIndex, final Date x, final Calendar cal)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setDouble(final int parameterIndex, final double x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setFloat(final int parameterIndex, final float x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setInt(final int parameterIndex, final int x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setLong(final int parameterIndex, final long x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setNCharacterStream(final int parameterIndex, final Reader value, final long length)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setNCharacterStream(final int parameterIndex, final Reader value)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setNClob(final int parameterIndex, final NClob value) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(
                SqlError.lookup(SqlError.UNSUPPORTED_TYPE, NClob.class.toString()));
    }

    @Override
    public void setNClob(final int parameterIndex, final Reader reader, final long length)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(
                SqlError.lookup(SqlError.UNSUPPORTED_TYPE, NClob.class.toString()));
    }

    @Override
    public void setNClob(final int parameterIndex, final Reader reader) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(
                SqlError.lookup(SqlError.UNSUPPORTED_TYPE, NClob.class.toString()));
    }

    @Override
    public void setNString(final int parameterIndex, final String value) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setNull(final int parameterIndex, final int sqlType, final String typeName)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setObject(final int parameterIndex, final Object x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType,
                          final int scaleOrLength)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setRef(final int parameterIndex, final Ref x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(
                SqlError.lookup(SqlError.UNSUPPORTED_TYPE, Ref.class.toString()));
    }

    @Override
    public void setRowId(final int parameterIndex, final RowId x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setSQLXML(final int parameterIndex, final SQLXML xmlObject) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(
                SqlError.lookup(SqlError.UNSUPPORTED_TYPE, SQLXML.class.toString()));
    }

    @Override
    public void setShort(final int parameterIndex, final short x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setString(final int parameterIndex, final String x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setTime(final int parameterIndex, final Time x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setTime(final int parameterIndex, final Time x, final Calendar cal)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setURL(final int parameterIndex, final URL x) throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }

    @Override
    public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length)
            throws SQLException {
        verifyOpen();
        throw new SQLFeatureNotSupportedException(SqlError.lookup(SqlError.PARAMETERS_NOT_SUPPORTED));
    }
}
