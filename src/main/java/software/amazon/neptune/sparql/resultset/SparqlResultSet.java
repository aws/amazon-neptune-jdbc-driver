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

package software.amazon.neptune.sparql.resultset;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public abstract class SparqlResultSet extends software.amazon.jdbc.ResultSet {
    private boolean wasNull = false;

    protected SparqlResultSet(final Statement statement, final List<String> columns,
                              final int rowCount) {
        super(statement, columns, rowCount);
    }

    @Override
    protected void doClose() throws SQLException {
    }

    @Override
    protected int getDriverFetchSize() throws SQLException {
        // TODO: AN-562 possibly raise error instead of leaving as a comment
        // Can't be done based on implementation.
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {
        // TODO: AN-562 possibly raise error instead of leaving as a comment
        // Can't be done based on implementation.
    }

    @Override
    public boolean wasNull() throws SQLException {
        return this.wasNull;
    }

    protected void setWasNull(final boolean wasNull) {
        this.wasNull = wasNull;
    }

    protected abstract Object getConvertedValue(int columnIndex) throws SQLException;

    protected abstract ResultSetMetaData getResultMetadata() throws SQLException;

}
