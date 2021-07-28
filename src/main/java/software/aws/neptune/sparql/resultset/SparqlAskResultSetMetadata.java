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

package software.aws.neptune.sparql.resultset;

import software.aws.neptune.jdbc.ResultSetMetaData;
import software.aws.neptune.jdbc.utilities.JdbcType;
import java.sql.SQLException;
import java.util.List;

public class SparqlAskResultSetMetadata extends ResultSetMetaData
        implements java.sql.ResultSetMetaData {

    private final Class<?> columnType;

    protected SparqlAskResultSetMetadata(final List<String> column, final Class<?> columnType) {
        super(column);
        this.columnType = columnType;
    }

    @Override
    public int getColumnType(final int column) throws SQLException {
        return JdbcType.BIT.getJdbcCode();
    }

    @Override
    public String getColumnTypeName(final int column) throws SQLException {
        return columnType.getName();
    }

    @Override
    public String getColumnClassName(final int column) throws SQLException {
        return columnType.getName();
    }
}
