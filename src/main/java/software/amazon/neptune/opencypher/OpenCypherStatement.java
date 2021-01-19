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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * OpenCypher implementation of DatabaseMetadata.
 */
public class OpenCypherStatement extends software.amazon.jdbc.Statement implements java.sql.Statement {
    private final OpenCypherQueryExecutor openCypherQueryExecutor;

    /**
     * OpenCypherStatement constructor, creates OpenCypherQueryExecutor and initializes super class.
     *
     * @param connection              Connection Object.
     */
    public OpenCypherStatement(final Connection connection) throws SQLException {
        super(connection);
        this.openCypherQueryExecutor = new OpenCypherQueryExecutor(
                new OpenCypherConnectionProperties(connection.getClientInfo()));
    }

    @Override
    protected void cancelQuery() throws SQLException {
        verifyOpen();
        openCypherQueryExecutor.cancelQuery();
    }

    @Override
    protected int getMaxFetchSize() throws SQLException {
        verifyOpen();
        return openCypherQueryExecutor.getMaxFetchSize();
    }

    @Override
    public java.sql.ResultSet executeQuery(final String sql) throws SQLException {
        return openCypherQueryExecutor.executeQuery(sql, this);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        verifyOpen();
        return openCypherQueryExecutor.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        verifyOpen();
        openCypherQueryExecutor.setQueryTimeout(seconds);
    }
}
