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

import com.google.common.collect.ImmutableList;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OpenCypherResultSetGetTables extends OpenCypherResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherResultSetGetTables.class);
    /**
     * TABLE_CAT String => table catalog (may be null)
     * TABLE_SCHEM String => table schema (may be null)
     * TABLE_NAME String => table name
     * TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
     * "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     * REMARKS String => explanatory comment on the table
     * TYPE_CAT String => the types catalog (may be null)
     * TYPE_SCHEM String => the types schema (may be null)
     * TYPE_NAME String => type name (may be null)
     * SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
     * REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
     */
    private static final List<String> KEYS = ImmutableList.of(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
            "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
    private static final Map<String, Object> MAPPED_KEYS = new HashMap<>();
    private static final List<String> EXPECTED_COLUMNS = ImmutableList.of("LABELS(n)");
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String LABELS_KEY = "LABELS(n)";

    static {
        MAPPED_KEYS.put("TABLE_CAT", null);
        MAPPED_KEYS.put("TABLE_SCHEM", null);
        MAPPED_KEYS.put("TABLE_TYPE", "TABLE");
        MAPPED_KEYS.put("REMARKS", "");
        MAPPED_KEYS.put("TYPE_CAT", null);
        MAPPED_KEYS.put("TYPE_SCHEM", null);
        MAPPED_KEYS.put("TYPE_NAME", null);
        MAPPED_KEYS.put("SELF_REFERENCING_COL_NAME", null);
        MAPPED_KEYS.put("REF_GENERATION", null);
    }

    private final List<Record> rows;

    /**
     * OpenCypherResultSetGetTables constructor, initializes super class.
     *
     * @param statement Statement Object.
     * @param session   Session Object.
     * @param result    Result Object.
     * @param rows      List of rows.
     * @param columns   List of columns.
     */
    public OpenCypherResultSetGetTables(final java.sql.Statement statement,
                                        final Session session,
                                        final Result result,
                                        final List<Record> rows,
                                        final List<String> columns) throws SQLException {
        super(statement, session, result, rows, columns);
        this.rows = rows;
        if (!columns.equals(EXPECTED_COLUMNS)) {
            throw new SQLException(String.format("Encountered unexpected return values for getTables function '%s'.",
                    columns.toString()));
        }
    }

    @Override
    protected java.sql.ResultSetMetaData getOpenCypherMetadata() throws SQLException {
        return new OpenCypherResultSetMetadata(KEYS, rows);
    }

    @Override
    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        verifyOpen();
        if (columnIndex <= KEYS.size() && columnIndex > 0) {
            final String key = KEYS.get(columnIndex - 1);
            if (MAPPED_KEYS.containsKey(key)) {
                return MAPPED_KEYS.get(key);
            } else if (key.equals(TABLE_NAME)) {
                final Value labels = rows.get(getRowIndex()).get(LABELS_KEY);
                if (!labels.type().equals(InternalTypeSystem.TYPE_SYSTEM.LIST())) {
                    throw new SQLException("Unexpected key encountered while getting schema.");
                }
                final List<?> objectLabels = labels.asList();
                if (objectLabels.stream().anyMatch(o -> !(o instanceof String))) {
                    throw new SQLException("Unexpected datatype encountered while getting schema..");
                }
                return String.join(":", (List<String>) objectLabels);
            }
        }
        throw SqlError
                .createSQLFeatureNotSupportedException(LOGGER, SqlError.INVALID_COLUMN_INDEX, columnIndex,
                        KEYS.size());
    }
}
