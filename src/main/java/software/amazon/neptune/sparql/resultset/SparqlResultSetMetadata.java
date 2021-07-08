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

import software.amazon.neptune.sparql.SparqlTypeMapping;
import java.sql.SQLException;
import java.util.List;

public class SparqlResultSetMetadata extends software.amazon.jdbc.ResultSetMetaData
        implements java.sql.ResultSetMetaData {

    private final List<Object> columnTypes;

    /**
     * SparqlResultSetMetadata constructor.
     *
     * @param columns     List of column names.
     * @param columnTypes List of column types.
     */
    protected SparqlResultSetMetadata(final List<String> columns, final List<Object> columnTypes) {
        super(columns);
        this.columnTypes = columnTypes;
    }

    /**
     * Get the class of a given column in Sparql Result.
     * Wrapped in Java class if available, otherwise outputs Jena XSDDatatype
     *
     * @param column the 1-based column index.
     * @return Bolt Type Object for column.
     */
    protected Object getColumnSparqlType(final int column) throws SQLException {
        verifyColumnIndex(column);
        // see if there are Sparql representation of mixed result
        return columnTypes.get(column - 1);
    }

    @Override
    public int getColumnType(final int column) throws SQLException {
        verifyColumnIndex(column);
        return SparqlTypeMapping.getJDBCType(getColumnSparqlType(column)).getJdbcCode();
    }

    @Override
    public String getColumnTypeName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return getColumnSparqlType(column).toString();
    }

    @Override
    public String getColumnClassName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return SparqlTypeMapping.getJavaType(getColumnSparqlType(column)).getName();
    }
}
