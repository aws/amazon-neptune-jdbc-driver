/*
 * Copyright <2023> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import software.aws.neptune.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import software.aws.neptune.common.ResultSetInfoWithoutRows;
import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetColumns;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparqlResultSetGetColumns extends ResultSetGetColumns implements java.sql.ResultSet {
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
     * ResultSetGetColumns constructor, initializes super class.
     *
     * @param statement                Statement Object.
     * @param gremlinSchema            GremlinSchema Object.
     * @param resultSetInfoWithoutRows ResultSetInfoWithoutRows Object.
     */
    public SparqlResultSetGetColumns(final Statement statement,
                                     final GremlinSchema gremlinSchema,
                                     final ResultSetInfoWithoutRows resultSetInfoWithoutRows)
            throws SQLException {
        super(statement, gremlinSchema, resultSetInfoWithoutRows);
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        final List<String> orderedColumns = getColumns();
        final List<Object> rowTypes = new ArrayList<>();
        for (final String column : orderedColumns) {
            rowTypes.add(COLUMN_TYPE_MAP.get(column));
        }
        return new SparqlResultSetMetadata(getColumns(), rowTypes);
    }
}
