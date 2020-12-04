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

import software.amazon.jdbc.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Mock implementation for ResultSet object so it can be instantiated and tested.
 */
public class MockResultSet extends ResultSet implements java.sql.ResultSet {
    private int rowIndex = 0;
    private static final int ROW_COUNT = 10;

    /**
     * Constructor for MockResultSet.
     * @param statement Statement Object.
     */
    public MockResultSet(final Statement statement) {
        super(statement);
    }

    @Override
    protected void doClose() throws SQLException {
    }

    @Override
    protected int getDriverFetchSize() throws SQLException {
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {
    }

    @Override
    protected int getRowIndex() {
        return rowIndex;
    }

    @Override
    protected int getRowCount() {
        return ROW_COUNT;
    }

    @Override
    public boolean next() throws SQLException {
        if (++rowIndex >= ROW_COUNT) {
            rowIndex = ROW_COUNT;
        }
        return (rowIndex < ROW_COUNT);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        return 0;
    }

    public void setRowIdx(final int idx) {
        this.rowIndex = idx;
    }
}
