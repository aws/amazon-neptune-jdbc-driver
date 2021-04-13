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

package software.amazon.jdbc.mock;

import software.amazon.jdbc.Statement;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Mock implementation for Statement object so it can be instantiated and tested.
 */
public class MockStatement extends Statement implements java.sql.Statement {
    private java.sql.ResultSet resultSet = null;

    /**
     * Constructor for MockStatement.
     *
     * @param connection Connection to pass to Statement.
     */
    public MockStatement(final Connection connection) throws SQLException {
        super(connection, new MockQueryExecutor());
    }

    public void setResultSet(final java.sql.ResultSet resultSet) {
        this.resultSet = resultSet;
    }
}
