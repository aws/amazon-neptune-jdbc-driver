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

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of javax.sql.PooledConnection for Neptune JDBC Driver.
 */
public class NeptunePooledConnection implements javax.sql.PooledConnection {
    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public void addConnectionEventListener(final ConnectionEventListener listener) {

    }

    @Override
    public void removeConnectionEventListener(final ConnectionEventListener listener) {

    }

    @Override
    public void addStatementEventListener(final StatementEventListener listener) {

    }

    @Override
    public void removeStatementEventListener(final StatementEventListener listener) {

    }
}
