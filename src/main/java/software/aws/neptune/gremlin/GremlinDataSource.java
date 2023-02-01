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

package software.aws.neptune.gremlin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.NeptuneDriver;
import software.aws.neptune.jdbc.DataSource;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.SqlError;
import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;

/**
 * Gremlin implementation of DataSource.
 */
public class GremlinDataSource extends DataSource
        implements javax.sql.DataSource, javax.sql.ConnectionPoolDataSource {
    public static final String GREMLIN_PREFIX = NeptuneDriver.CONN_STRING_PREFIX + "gremlin://";
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinDataSource.class);

    private final GremlinConnectionProperties connectionProperties;

    /**
     * GremlinDataSource constructor, initializes super class.
     */
    GremlinDataSource() throws SQLException {
        super();
        this.connectionProperties = new GremlinConnectionProperties();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(GREMLIN_PREFIX, connectionProperties);
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public PooledConnection getPooledConnection() throws SQLException {
        return new GremlinPooledConnection(getConnection());
    }

    @Override
    public PooledConnection getPooledConnection(final String user, final String password) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    /**
     * Sets the timeout for opening a connection.
     *
     * @return the connection timeout in seconds.
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        return connectionProperties.getConnectionTimeoutMillis();
    }

    /**
     * Sets the timeout for opening a connection.
     *
     * @param seconds The connection timeout in seconds.
     * @throws SQLException if timeout is negative.
     */
    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {
        connectionProperties.setConnectionTimeoutMillis(seconds);
    }

    /**
     * Gets the application name.
     *
     * @return The application name.
     */
    public String getApplicationName() {
        return connectionProperties.getApplicationName();
    }

    /**
     * Sets the application name.
     *
     * @param applicationName The application name.
     * @throws SQLException if value is invalid.
     */
    public void setApplicationName(final String applicationName) throws SQLException {
        connectionProperties.setApplicationName(applicationName);
    }

    /**
     * Gets the connection endpoint.
     *
     * @return The connection endpoint.
     */
    public String getContactPoint() {
        return connectionProperties.getContactPoint();
    }

    /**
     * Sets the connection endpoint.
     *
     * @param endpoint The connection endpoint.
     * @throws SQLException if value is invalid.
     */
    public void setContactPoint(final String endpoint) throws SQLException {
        connectionProperties.setContactPoint(endpoint);
    }

    /**
     * Gets the connection timeout in milliseconds.
     *
     * @return The connection timeout in milliseconds.
     */
    public int getConnectionTimeoutMillis() {
        return connectionProperties.getConnectionTimeoutMillis();
    }

    /**
     * Sets the connection timeout in milliseconds.
     *
     * @param timeoutMillis The connection timeout in milliseconds.
     * @throws SQLException if value is invalid.
     */
    public void setConnectionTimeoutMillis(final int timeoutMillis) throws SQLException {
        connectionProperties.setConnectionTimeoutMillis(timeoutMillis);
    }

    /**
     * Gets the connection retry count.
     *
     * @return The connection retry count.
     */
    public int getConnectionRetryCount() {
        return connectionProperties.getConnectionRetryCount();
    }

    /**
     * Sets the connection retry count.
     *
     * @param retryCount The connection retry count.
     * @throws SQLException if value is invalid.
     */
    public void setConnectionRetryCount(final int retryCount) throws SQLException {
        connectionProperties.setConnectionRetryCount(retryCount);
    }

    /**
     * Gets the authentication scheme.
     *
     * @return The authentication scheme.
     */
    public AuthScheme getAuthScheme() {
        return connectionProperties.getAuthScheme();
    }

    /**
     * Sets the authentication scheme.
     *
     * @param authScheme The authentication scheme.
     * @throws SQLException if value is invalid.
     */
    public void setAuthScheme(final AuthScheme authScheme) throws SQLException {
        connectionProperties.setAuthScheme(authScheme);
    }

    /**
     * Gets the use encryption.
     *
     * @return The use encryption.
     */
    public boolean getEnableSsl() {
        return connectionProperties.getEnableSsl();
    }

    /**
     * Sets the use encryption.
     *
     * @param useEncryption The use encryption.
     */
    public void setEnableSsl(final boolean useEncryption) throws SQLClientInfoException {
        connectionProperties.setEnableSsl(useEncryption);
    }
}
