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

package software.aws.neptune.jdbc.mock;

import software.aws.neptune.jdbc.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Mock implementation for ResultSet object so it can be instantiated and tested.
 */
public class MockResultSet extends ResultSet implements java.sql.ResultSet {
    private static final int ROW_COUNT = 10;
    private static final int COL_COUNT = 10;
    private int rowIndex = 0;

    /**
     * Constructor for MockResultSet.
     *
     * @param statement Statement Object.
     */
    public MockResultSet(final Statement statement) {
        super(statement, new ArrayList<>(10), ROW_COUNT);
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
    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        if (columnIndex == 0 || columnIndex > COL_COUNT) {
            throw new SQLException("Index out of bounds.");
        }
        return null;
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        return null;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    public void setRowIdx(final int idx) {
        this.rowIndex = idx;
    }
}
