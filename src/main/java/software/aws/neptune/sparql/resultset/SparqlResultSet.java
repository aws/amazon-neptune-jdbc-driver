/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.sparql.resultset;

import software.aws.neptune.jdbc.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public abstract class SparqlResultSet extends ResultSet {
    private boolean wasNull = false;

    protected SparqlResultSet(final Statement statement, final List<String> columns,
                              final int rowCount) {
        super(statement, columns, rowCount);
    }

    @Override
    protected void doClose() throws SQLException {
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
