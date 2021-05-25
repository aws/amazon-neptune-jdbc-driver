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

import software.amazon.neptune.common.ResultSetInfoWithoutRows;
import software.amazon.neptune.common.gremlindatamodel.GraphSchema;
import software.amazon.neptune.common.gremlindatamodel.resultset.ResultSetGetTables;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Gremlin ResultSet class for getTables.
 */
public class GremlinResultSetGetTables extends ResultSetGetTables implements java.sql.ResultSet {
    private static final Map<String, Class<?>> COLUMN_TYPE_MAP = new HashMap<>();

    static {
        COLUMN_TYPE_MAP.put("TABLE_CAT", String.class);
        COLUMN_TYPE_MAP.put("TABLE_SCHEM", String.class);
        COLUMN_TYPE_MAP.put("TABLE_NAME", String.class);
        COLUMN_TYPE_MAP.put("COLUMN_NAME", String.class);
        COLUMN_TYPE_MAP.put("DATA_TYPE", Integer.class);
        COLUMN_TYPE_MAP.put("TYPE_NAME", String.class);
        COLUMN_TYPE_MAP.put("COLUMN_SIZE", Integer.class);
        COLUMN_TYPE_MAP.put("BUFFER_LENGTH", Integer.class);
        COLUMN_TYPE_MAP.put("DECIMAL_DIGITS", Integer.class);
        COLUMN_TYPE_MAP.put("NUM_PREC_RADIX", Integer.class);
        COLUMN_TYPE_MAP.put("NULLABLE", Integer.class);
        COLUMN_TYPE_MAP.put("REMARKS", String.class);
        COLUMN_TYPE_MAP.put("COLUMN_DEF", String.class);
        COLUMN_TYPE_MAP.put("SQL_DATA_TYPE", Integer.class);
        COLUMN_TYPE_MAP.put("SQL_DATETIME_SUB", Integer.class);
        COLUMN_TYPE_MAP.put("CHAR_OCTET_LENGTH", Integer.class);
        COLUMN_TYPE_MAP.put("ORDINAL_POSITION", Integer.class);
        COLUMN_TYPE_MAP.put("IS_NULLABLE", String.class);
        COLUMN_TYPE_MAP.put("SCOPE_CATALOG", String.class);
        COLUMN_TYPE_MAP.put("SCOPE_SCHEMA", String.class);
        COLUMN_TYPE_MAP.put("SCOPE_TABLE", String.class);
        COLUMN_TYPE_MAP.put("SOURCE_DATA_TYPE", Integer.class);
        COLUMN_TYPE_MAP.put("IS_AUTOINCREMENT", String.class);
        COLUMN_TYPE_MAP.put("IS_GENERATEDCOLUMN", String.class);
    }

    /**
     * OpenCypherResultSetGetColumns constructor, initializes super class.
     *
     * @param statement                Statement Object.
     * @param graphSchemas             List of GraphSchema Objects.
     * @param resultSetInfoWithoutRows ResultSetInfoWithoutRows Object.
     */
    public GremlinResultSetGetTables(final Statement statement,
                                     final List<GraphSchema> graphSchemas,
                                     final ResultSetInfoWithoutRows resultSetInfoWithoutRows)
            throws SQLException {
        super(statement, graphSchemas, resultSetInfoWithoutRows);
    }

    @Override
    protected ResultSetMetaData getResultMetadata() {
        final List<String> orderedColumns = getColumns();
        final List<Class<?>> rowTypes = new ArrayList<>();
        for (int i = 0; i < orderedColumns.size(); i++) {
            rowTypes.add(COLUMN_TYPE_MAP.get(orderedColumns.get(i)));
        }
        return new GremlinResultSetMetadata(orderedColumns, rowTypes);
    }
}
