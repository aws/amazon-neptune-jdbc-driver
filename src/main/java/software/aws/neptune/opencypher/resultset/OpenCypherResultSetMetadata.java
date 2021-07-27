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

package software.aws.neptune.opencypher.resultset;

import org.neo4j.driver.types.Type;
import software.aws.jdbc.ResultSetMetaData;
import software.aws.neptune.opencypher.OpenCypherTypeMapping;
import java.sql.SQLException;
import java.util.List;

/**
 * OpenCypher implementation of ResultSetMetadata.
 */
public class OpenCypherResultSetMetadata extends ResultSetMetaData
        implements java.sql.ResultSetMetaData {
    private final List<Type> columnTypes;

    /**
     * OpenCypherResultSetMetadata constructor.
     *
     * @param columns     List of column names.
     * @param columnTypes List of column types.
     */
    public OpenCypherResultSetMetadata(final List<String> columns, final List<Type> columnTypes) {
        super(columns);
        this.columnTypes = columnTypes;
    }

    /**
     * Get Bolt type of a given column.
     *
     * @param column the 1-based column index.
     * @return Bolt Type Object for column.
     */
    protected Type getColumnBoltType(final int column) {
        // TODO: Loop rows to find common type and cache it.
        return columnTypes.get(column - 1);
    }

    @Override
    public int getColumnType(final int column) throws SQLException {
        verifyColumnIndex(column);
        return OpenCypherTypeMapping.BOLT_TO_JDBC_TYPE_MAP.get(getColumnBoltType(column)).getJdbcCode();
    }

    @Override
    public String getColumnTypeName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return getColumnBoltType(column).name();
    }

    @Override
    public String getColumnClassName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return OpenCypherTypeMapping.BOLT_TO_JAVA_TYPE_MAP.get(getColumnBoltType(column)).getName();
    }
}
