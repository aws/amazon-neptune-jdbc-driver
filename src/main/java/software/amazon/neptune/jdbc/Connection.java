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

import software.amazon.neptune.jdbc.opencypher.OpenCypherPreparedStatement;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract implementation of java.sql.Connection for Neptune JDBC Driver.
 * Concrete implementations will be provided in query language specific implementations.
 */
public abstract class Connection implements java.sql.Connection {
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    @Override
    public Statement createStatement() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        verifyOpen();
        return new OpenCypherPreparedStatement();
    }

    @Override
    public CallableStatement prepareCall(final String sql) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public String nativeSQL(final String sql) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        verifyOpen();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        verifyOpen();
        return false;
    }

    @Override
    public void commit() throws SQLException {
        // TODO: Throw SQLFeatureNotSupportedException. We may be able to implement this later though.
    }

    @Override
    public void rollback() throws SQLException {
        // TODO: Throw SQLFeatureNotSupportedException. We may be able to implement this later though.
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed.get();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {
        verifyOpen();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        verifyOpen();
        return false;
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        verifyOpen();
    }

    @Override
    public String getCatalog() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public void setTransactionIsolation(final int level) throws SQLException {
        verifyOpen();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        verifyOpen();
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        verifyOpen();
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
        verifyOpen();
    }

    @Override
    public void setHoldability(final int holdability) throws SQLException {
        verifyOpen();
    }

    @Override
    public int getHoldability() throws SQLException {
        verifyOpen();
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public Savepoint setSavepoint(final String name) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public void rollback(final Savepoint savepoint) throws SQLException {
        verifyOpen();

    }

    @Override
    public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
        // TODO: Throw SQLFeatureNotSupportedException.
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        return false;
    }

    @Override
    public void setClientInfo(final String name, final String value) throws SQLClientInfoException {
        // if (isClosed.get()) {
        // TODO: Throw exception separately for this because it throws SQLClientInfoException.
        // }
    }

    @Override
    public void setClientInfo(final Properties properties) throws SQLClientInfoException {
        // if (isClosed.get()) {
        // TODO: Throw exception separately for this because it throws SQLClientInfoException.
        // }
    }

    @Override
    public String getClientInfo(final String name) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public Struct createStruct(final String typeName, final Object[] attributes) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public void setSchema(final String schema) throws SQLException {
        verifyOpen();
    }

    @Override
    public String getSchema() throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public void abort(final Executor executor) throws SQLException {
        verifyOpen();
    }

    @Override
    public void setNetworkTimeout(final Executor executor, final int milliseconds) throws SQLException {
        verifyOpen();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        verifyOpen();
        return 0;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        verifyOpen();
        return null;
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return false;
    }

    private void verifyOpen() {
        //if (isClosed.get()) {
        // TODO: Throw SQLException.
        //}
    }
}
