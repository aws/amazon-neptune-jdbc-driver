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

package software.amazon.jdbc.mock;

import software.amazon.jdbc.PooledConnection;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Mock implementation for PooledConnection object so it can be instantiated and tested.
 */
public class MockPooledConnection extends PooledConnection implements javax.sql.PooledConnection {

    /**
     * MockPooledConnection constructor.
     *
     * @param connection Connection Object.
     */
    public MockPooledConnection(final Connection connection) {
        super(connection);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }
}
