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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SparqlResultSet extends software.amazon.jdbc.ResultSet implements java.sql.ResultSet {

    private final ResultSet result;
    private final List<QuerySolution> rows;
    private final List<String> columns;

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

    @AllArgsConstructor
    @Getter
    public static class ResultSetInfoWithRows {
        private final ResultSet result;
        private final List<QuerySolution> rows;
        private final List<String> columns;
    }
}
