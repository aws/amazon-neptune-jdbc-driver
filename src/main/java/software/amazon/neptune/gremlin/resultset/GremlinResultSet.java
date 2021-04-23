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

package software.amazon.neptune.gremlin.resultset;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.neptune.common.ResultSetInfoWithoutRows;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Gremlin ResultSet class.
 */
public class GremlinResultSet extends software.amazon.jdbc.ResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinResultSet.class);
    private final List<String> columns;
    private final List<Map<String, Object>> rows;
    private final boolean wasNull = false;

    /**
     * GremlinResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfo ResultSetInfoWithRows Object.
     */
    public GremlinResultSet(final java.sql.Statement statement, final ResultSetInfoWithRows resultSetInfo) {
        super(statement, resultSetInfo.getColumns(), resultSetInfo.getRows().size());
        this.columns = resultSetInfo.getColumns();
        this.rows = resultSetInfo.getRows();
    }

    /**
     * GremlinResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfo ResultSetInfoWithoutRows Object.
     */
    public GremlinResultSet(final java.sql.Statement statement, final ResultSetInfoWithoutRows resultSetInfo) {
        super(statement, resultSetInfo.getColumns(), resultSetInfo.getRowCount());
        this.columns = resultSetInfo.getColumns();
        this.rows = null;
    }

    @Override
    protected void doClose() throws SQLException {
        // TODO.
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
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        final List<Class<?>> rowTypes = new ArrayList<>();
        // TODO: Fix type support here, we need to go through and figure out what the types are.
        for (int i = 0; i < columns.size(); i++) {
            rowTypes.add(String.class);
        }
        return new GremlinResultSetMetadata(columns, rowTypes);
    }

    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        // Get current row from map.

        // Get string name of column from column list.

        // Check if map contains column name.
        // If not set wasNull and return null.
        // Else convert to primitive Java type and return (Integer, String, etc)
        return null;
    }

    @Override
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
        LOGGER.trace("Getting column {} as an Object using provided Map.", columnIndex);
        // TODO
        return null;
    }

    @AllArgsConstructor
    @Getter
    public static class ResultSetInfoWithRows {
        private final List<Map<String, Object>> rows;
        private final List<String> columns;
    }
}
