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

package software.aws.jdbc.mock;

import software.aws.jdbc.DataSource;
import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Mock implementation for DataSource object so it can be instantiated and tested.
 */
public class MockDataSource extends DataSource implements javax.sql.DataSource {
    @Override
    public PooledConnection getPooledConnection() throws SQLException {
        return null;
    }

    @Override
    public PooledConnection getPooledConnection(final String user, final String password) throws SQLException {
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        return null;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {

    }
}
