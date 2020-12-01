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

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * OpenCypher implementation of ResultSet.
 */
public class OpenCypherResultSet extends software.amazon.jdbc.ResultSet implements java.sql.ResultSet {
    private final Result result;
    private final List<String> columns;
    private final List<Record> rows;
    private final boolean wasNull = false;
    private int rowIndex = -1;

    /**
     * OpenCypherResultSet constructor, initializes super class.
     * @param statement Statement Object.
     * @param result Result Object.
     */
    OpenCypherResultSet(final java.sql.Statement statement, final Result result) {
        super(statement);
        this.result = result;
        this.rows = result.list();
        this.columns = result.keys();
    }

    @Override
    protected void doClose() {
        result.consume();
    }

    @Override
    protected int getDriverFetchSize() throws SQLException {
        // Do we want to update this or statement?
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {
        // Do we want to update this or statement?
    }

    @Override
    protected int getRowIndex() {
        return this.rowIndex;
    }

    @Override
    protected int getRowCount() {
        return rows.size();
    }

    @Override
    public boolean next() throws SQLException {
        // Increment row index, if it exceeds capacity, set it to 1 after the last element.
        if (++this.rowIndex >= rows.size()) {
            this.rowIndex = rows.size();
        }
        return (this.rowIndex < rows.size());
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        return columns.indexOf(columnLabel);
    }
}
