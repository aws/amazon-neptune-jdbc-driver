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

import software.amazon.jdbc.utilities.ConnectionProperties;
import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;


/**
 * OpenCypher implementation of DataSource.
 */
public class OpenCypherDataSource extends software.amazon.jdbc.DataSource implements javax.sql.DataSource, javax.sql.ConnectionPoolDataSource {
    // TODO: perhaps this should be a ConnectionProperties object
    private final Properties properties;

    /**
     * OpenCypherDataSource constructor, initializes super class.
     * @param properties Properties Object.
     */
    OpenCypherDataSource(final Properties properties) {
        super();
        this.properties = (Properties) properties.clone();
    }

    @Override
    public java.sql.Connection getConnection() throws SQLException {
        return new OpenCypherConnection(new ConnectionProperties(properties));
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        // TODO: Add some auth logic.
        return null;
    }

    @Override
    public PooledConnection getPooledConnection() throws SQLException {
        return new OpenCypherPooledConnection(getConnection());
    }

    @Override
    public PooledConnection getPooledConnection(final String user, final String password) throws SQLException {
        return new OpenCypherPooledConnection(getConnection(user, password));
    }

    // TODO: Implement
    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }
}
