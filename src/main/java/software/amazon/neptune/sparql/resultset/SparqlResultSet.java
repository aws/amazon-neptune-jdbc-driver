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

package software.amazon.neptune.sparql.resultset;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SparqlResultSet extends software.amazon.jdbc.ResultSet implements java.sql.ResultSet {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlResultSet.class);
    private final ResultSet result;
    private final List<QuerySolution> rows;
    private final List<String> columns;
    private boolean wasNull = false;

    /**
     * SparqlResultSet constructor, initializes super class.
     *
     * @param statement     Statement Object.
     * @param resultSetInfo ResultSetInfoWithRows Object.
     */
    public SparqlResultSet(final Statement statement, final ResultSetInfoWithRows resultSetInfo) {
        super(statement, resultSetInfo.getColumns(), resultSetInfo.getRows().size());
        this.result = resultSetInfo.getResult();
        this.rows = resultSetInfo.getRows();
        this.columns = resultSetInfo.getColumns();
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
        final Object value = getValue(columnIndex);
        return value.toString();
    }

    private Object getValue(final int columnIndex) throws SQLException {
        verifyOpen();
        if (rows == null) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.UNSUPPORTED_RESULT_SET_TYPE);
        }
        validateRowColumn(columnIndex);

        final String colName = columns.get(columnIndex - 1);
        final QuerySolution row = rows.get(getRowIndex());
        final Object value = row.get(colName); // of type RDFNode
        wasNull = (value == null);

        return value;
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        return null;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @AllArgsConstructor
    @Getter
    public static class ResultSetInfoWithRows {
        private final ResultSet result;
        private final List<QuerySolution> rows;
        private final List<String> columns;
    }
}
