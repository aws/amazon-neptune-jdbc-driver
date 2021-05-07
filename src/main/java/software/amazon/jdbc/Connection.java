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

import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.CastHelper;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.jdbc.utilities.QueryExecutor;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import java.sql.Array;
import java.sql.ClientInfoStatus;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract implementation of Connection for JDBC Driver.
 */
public abstract class Connection implements java.sql.Connection {
    private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final ConnectionProperties connectionProperties;
    private Map<String, Class<?>> typeMap = new HashMap<>();
    private SQLWarning warnings = null;

    protected Connection(@NonNull final ConnectionProperties connectionProperties) throws SQLException {
        this.connectionProperties = connectionProperties;
        this.connectionProperties.putIfAbsent(
                ConnectionProperties.APPLICATION_NAME_KEY,
                Driver.APPLICATION_NAME);
        setLogLevel();
    }

    /**
     * Function to get QueryExecutor of underlying connection.
     *
     * @return QueryExecutor Object.
     */
    public abstract QueryExecutor getQueryExecutor();

    protected ConnectionProperties getConnectionProperties() {
        return this.connectionProperties;
    }

    private void setLogLevel() {
        LogManager.getRootLogger().setLevel(connectionProperties.getLogLevel());
    }

    private Map<String, ClientInfoStatus> getFailures(@NonNull final String name, final String value) {
        final Properties newProperties = new Properties();
        newProperties.setProperty(name, value);

        return getFailures(newProperties);
    }

    private Map<String, ClientInfoStatus> getFailures(final Properties properties) {
        final Map<String, ClientInfoStatus> clientInfoStatusMap = new HashMap<>();
        if (properties != null) {
            for (final String name : properties.stringPropertyNames()) {
                clientInfoStatusMap.put(name, ClientInfoStatus.REASON_UNKNOWN);
            }
        }
        return clientInfoStatusMap;
    }

    /*
        Functions that have their implementation in this Connection class.
     */
    @Override
    public Properties getClientInfo() throws SQLException {
        verifyOpen();
        final Properties clientInfo = new Properties();
        clientInfo.putAll(connectionProperties);
        return clientInfo;
    }

    @SneakyThrows
    @Override
    public void setClientInfo(final Properties properties) throws SQLClientInfoException {
        if (isClosed.get()) {
            throw SqlError.createSQLClientInfoException(
                    LOGGER,
                    getFailures(properties),
                    SqlError.CONN_CLOSED);
        }
        if (properties != null) {
            for (final String name : properties.stringPropertyNames()) {
                final String value = properties.getProperty(name);
                setClientInfo(name, value);
            }
        }
        LOGGER.debug("Successfully set client info with all properties.");
    }

    @Override
    public String getClientInfo(final String name) throws SQLException {
        verifyOpen();
        if (name == null || !connectionProperties.isSupportedProperty(name)) {
            LOGGER.debug("Invalid name '{}' passed to getClientInfo, returning null for value.", name);
            return null;
        }
        return connectionProperties.get(name).toString();
    }

    @Override
    public void setClientInfo(@NonNull final String name, final String value) throws SQLClientInfoException {
        if (isClosed.get()) {
            throw SqlError.createSQLClientInfoException(
                    LOGGER,
                    getFailures(name, value),
                    SqlError.CONN_CLOSED);
        }

        if (!connectionProperties.isSupportedProperty(name)) {
            throw SqlError.createSQLClientInfoException(
                    LOGGER,
                    getFailures(name, value),
                    SqlError.INVALID_CONNECTION_PROPERTY, name, value);
        }

        try {
            if (value != null) {
                connectionProperties.validateAndSetProperty(name, value);
                LOGGER.debug("Successfully set client info with name '{}' and value '{}'", name, value);
            } else {
                connectionProperties.remove(name);
                LOGGER.debug("Successfully removed client info with name '{}'", name);
            }
        } catch (final SQLException ex) {
            throw SqlError.createSQLClientInfoException(
                    LOGGER,
                    getFailures(name, value),
                    ex);
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        verifyOpen();
        return typeMap;
    }

    @Override
    public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
        verifyOpen();
        if (map == null) {
            LOGGER.debug("Null value is passed as conversion map, failing back to an empty hash map.");
            typeMap = new HashMap<>();
        } else {
            typeMap = map;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return CastHelper.isWrapperFor(iface, this);
    }

    @Override
    public String nativeSQL(final String sql) throws SQLException {
        verifyOpen();
        return sql;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return CastHelper.unwrap(iface, LOGGER, this);
    }

    @Override
    public void clearWarnings() throws SQLException {
        verifyOpen();
        warnings = null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        verifyOpen();
        return warnings;
    }

    /**
     * Set a new warning if there were none, or add a new warning to the end of the list.
     *
     * @param warning the {@link SQLWarning} to be set.SQLError
     */
    protected void addWarning(final SQLWarning warning) {
        LOGGER.warn(warning.getMessage());
        if (this.warnings == null) {
            this.warnings = warning;
            return;
        }
        this.warnings.setNextWarning(warning);
    }

    protected abstract void doClose();

    @Override
    public void close() {
        if (!isClosed.getAndSet(true)) {
            doClose();
        }
    }

    /**
     * Verify the connection is open.
     *
     * @throws SQLException if the connection is closed.
     */
    protected void verifyOpen() throws SQLException {
        if (isClosed.get()) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.CONN_CLOSED);
        }
    }

    // Add default implementation of create functions which throw.
    @Override
    public Struct createStruct(final String typeName, final Object[] attributes) throws SQLException {
        // Only reason to do this is for parameters, if you do not support them then this is a safe implementation.
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public java.sql.Blob createBlob() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public java.sql.Clob createClob() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public java.sql.NClob createNClob() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
        // Even though Arrays are supported, the only reason to create an Array in the application is to pass it as
        // a parameter which is not supported.
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }


    // Add default of no schema and no catalog support.
    @Override
    public String getSchema() throws SQLException {
        // No schema support. Return null.
        return null;
    }

    @Override
    public void setSchema(final String schema) throws SQLException {
        // No schema support. Do nothing.
    }

    @Override
    public String getCatalog() throws SQLException {
        // No catalog support. Return null.
        return null;
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        // No catalog support. Do nothing.
    }

    // Add default read-only and autocommit only implementation.
    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        // Fake allowing autoCommit to be turned off, even though transactions are not supported, as some applications
        // turn this off without checking support.
        LOGGER.debug("Transactions are not supported, do nothing for setAutoCommit.");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {
        if (!readOnly) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
        }
    }

    // Default to forward only with read only concurrency.
    @Override
    public java.sql.Statement createStatement() throws SQLException {
        verifyOpen();
        return createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    // Add default no transaction support statement.
    @Override
    public java.sql.Statement createStatement(final int resultSetType, final int resultSetConcurrency,
                                              final int resultSetHoldability) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys)
            throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql, final int resultSetType,
                                                       final int resultSetConcurrency)
            throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql, final int resultSetType,
                                                       final int resultSetConcurrency,
                                                       final int resultSetHoldability) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql, final int[] columnIndexes)
            throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql, final String[] columnNames)
            throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    // Add default no callable statement support.
    @Override
    public java.sql.CallableStatement prepareCall(final String sql) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public java.sql.CallableStatement prepareCall(final String sql, final int resultSetType,
                                                  final int resultSetConcurrency)
            throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public java.sql.CallableStatement prepareCall(final String sql, final int resultSetType,
                                                  final int resultSetConcurrency,
                                                  final int resultSetHoldability) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    // Default transactions as unsupported.
    @Override
    public int getTransactionIsolation() throws SQLException {
        return java.sql.Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(final int level) throws SQLException {
        verifyOpen();
        if (level != java.sql.Connection.TRANSACTION_NONE) {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
        }
    }

    @Override
    public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void rollback() throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void rollback(final Savepoint savepoint) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public Savepoint setSavepoint(final String name) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void abort(final Executor executor) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void commit() throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public int getHoldability() throws SQLException {
        return java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public void setHoldability(final int holdability) throws SQLException {
        verifyOpen();
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public java.sql.Statement createStatement(final int resultSetType, final int resultSetConcurrency)
            throws SQLException {
        return new software.amazon.jdbc.Statement(this, getQueryExecutor());
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql) throws SQLException {
        return new software.amazon.jdbc.PreparedStatement(this, sql, getQueryExecutor());
    }

    @Override
    public int getNetworkTimeout() throws SQLFeatureNotSupportedException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public void setNetworkTimeout(final Executor executor, final int milliseconds)
            throws SQLFeatureNotSupportedException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        return getQueryExecutor().isValid(timeout);
    }
}

