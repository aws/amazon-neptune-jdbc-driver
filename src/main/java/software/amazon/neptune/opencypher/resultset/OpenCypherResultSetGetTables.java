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

package software.amazon.neptune.opencypher.resultset;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;
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
 * OpenCypher ResultSet class for getTables.
 */
public class OpenCypherResultSetGetTables extends ResultSetGetTables implements java.sql.ResultSet {
    private static final Map<String, Type> COLUMN_TYPE_MAP = new HashMap<>();

    static {
        COLUMN_TYPE_MAP.put("TABLE_CAT", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("TABLE_SCHEM", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("TABLE_NAME", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("COLUMN_NAME", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("DATA_TYPE", InternalTypeSystem.TYPE_SYSTEM.INTEGER());
        COLUMN_TYPE_MAP.put("TYPE_NAME", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("COLUMN_SIZE", InternalTypeSystem.TYPE_SYSTEM.INTEGER());
        COLUMN_TYPE_MAP.put("BUFFER_LENGTH", InternalTypeSystem.TYPE_SYSTEM.INTEGER());
        COLUMN_TYPE_MAP.put("DECIMAL_DIGITS", InternalTypeSystem.TYPE_SYSTEM.INTEGER());
        COLUMN_TYPE_MAP.put("NUM_PREC_RADIX", InternalTypeSystem.TYPE_SYSTEM.INTEGER());
        COLUMN_TYPE_MAP.put("NULLABLE", InternalTypeSystem.TYPE_SYSTEM.INTEGER());
        COLUMN_TYPE_MAP.put("REMARKS", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("COLUMN_DEF", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("SQL_DATA_TYPE", InternalTypeSystem.TYPE_SYSTEM.INTEGER());
        COLUMN_TYPE_MAP.put("SQL_DATETIME_SUB", InternalTypeSystem.TYPE_SYSTEM.INTEGER());
        COLUMN_TYPE_MAP.put("CHAR_OCTET_LENGTH", InternalTypeSystem.TYPE_SYSTEM.INTEGER());
        COLUMN_TYPE_MAP.put("ORDINAL_POSITION", InternalTypeSystem.TYPE_SYSTEM.INTEGER());
        COLUMN_TYPE_MAP.put("IS_NULLABLE", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("SCOPE_CATALOG", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("SCOPE_SCHEMA", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("SCOPE_TABLE", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("SOURCE_DATA_TYPE", InternalTypeSystem.TYPE_SYSTEM.INTEGER());
        COLUMN_TYPE_MAP.put("IS_AUTOINCREMENT", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("IS_GENERATEDCOLUMN", InternalTypeSystem.TYPE_SYSTEM.STRING());
    }

    /**
     * OpenCypherResultSetGetColumns constructor, initializes super class.
     *
     * @param statement                Statement Object.
     * @param graphSchemas             List of NodeColumnInfo Objects.
     * @param resultSetInfoWithoutRows ResultSetInfoWithoutRows Object.
     */
    public OpenCypherResultSetGetTables(final Statement statement,
                                        final List<GraphSchema> graphSchemas,
                                        final ResultSetInfoWithoutRows resultSetInfoWithoutRows)
            throws SQLException {
        super(statement, graphSchemas, resultSetInfoWithoutRows);
    }

    @Override
    protected ResultSetMetaData getResultMetadata() {
        final List<String> orderedColumns = getColumns();
        final List<Type> rowTypes = new ArrayList<>();
        for (int i = 0; i < orderedColumns.size(); i++) {
            rowTypes.add(InternalTypeSystem.TYPE_SYSTEM.STRING());
        }
        return new OpenCypherResultSetMetadata(orderedColumns, rowTypes);
    }
}
