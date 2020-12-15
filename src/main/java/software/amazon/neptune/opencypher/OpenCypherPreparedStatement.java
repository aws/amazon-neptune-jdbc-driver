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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * OpenCypher implementation of PreparedStatement.
 */
public class OpenCypherPreparedStatement extends software.amazon.jdbc.PreparedStatement
        implements java.sql.PreparedStatement {
    private final OpenCypherQueryExecutor openCypherQueryExecutor;
    private final String sql;
    private java.sql.ResultSet resultSet = null;

    /**
     * OpenCypherPreparedStatement constructor, creates OpenCypherQueryExecutor and initializes super class.
     * @param connection Connection Object.
     * @param sql Sql query.
     * @param openCypherQueryExecutor Query executor.
     */
    public OpenCypherPreparedStatement(final java.sql.Connection connection, final String sql, final OpenCypherQueryExecutor openCypherQueryExecutor) {
        super(connection, sql);
        this.openCypherQueryExecutor = openCypherQueryExecutor;
        this.sql = sql;
    }


    @Override
    protected void cancelQuery() throws SQLException {
        verifyOpen();
        openCypherQueryExecutor.cancelQuery();
        // TODO: Async query execution and cancellation.
    }

    @Override
    protected int getMaxFetchSize() throws SQLException {
        verifyOpen();
        return openCypherQueryExecutor.getMaxFetchSize();
    }

    @Override
    public java.sql.ResultSet executeQuery() throws SQLException {
        resultSet = openCypherQueryExecutor.executeQuery(sql, this);
        return resultSet;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return (resultSet == null) ? null : resultSet.getMetaData();
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
