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

import software.aws.jdbc.utilities.QueryExecutor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MockQueryExecutor extends QueryExecutor {
    @Override
    public int getMaxFetchSize() {
        return 0;
    }

    @Override
    public boolean isValid(final int timeout) {
        return false;
    }

    @Override
    public ResultSet executeQuery(final String sql, final Statement statement) throws SQLException {
        return null;
    }

    @Override
    public ResultSet executeGetTables(final Statement statement, final String tableName) throws SQLException {
        return null;
    }

    @Override
    public ResultSet executeGetSchemas(final Statement statement) throws SQLException {
        return null;
    }

    @Override
    public ResultSet executeGetCatalogs(final Statement statement) throws SQLException {
        return null;
    }

    @Override
    public ResultSet executeGetTableTypes(final Statement statement) throws SQLException {
        return null;
    }

    @Override
    public ResultSet executeGetColumns(final Statement statement, final String nodes) throws SQLException {
        return null;
    }

    @Override
    protected <T> T runQuery(final String query) throws SQLException {
        return null;
    }

    @Override
    protected void performCancel() throws SQLException {
    }
}
