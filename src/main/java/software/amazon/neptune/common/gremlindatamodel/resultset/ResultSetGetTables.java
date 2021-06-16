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

package software.amazon.neptune.common.gremlindatamodel.resultset;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import software.amazon.neptune.common.ResultSetInfoWithoutRows;
import software.amazon.neptune.common.gremlindatamodel.GraphSchema;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base ResultSet for getTables.
 */
public abstract class ResultSetGetTables extends GenericResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultSetGetTables.class);
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
    private static final List<String> ORDERED_COLUMNS = ImmutableList.of(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
            "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
    private static final Map<String, Object> MAPPED_KEYS = new HashMap<>();
    private static final String TABLE_NAME = "TABLE_NAME";

    static {
        MAPPED_KEYS.put("TABLE_CAT", "catalog");
        MAPPED_KEYS.put("TABLE_SCHEM", "");
        MAPPED_KEYS.put("TABLE_TYPE", "TABLE");
        MAPPED_KEYS.put("REMARKS", "");
        MAPPED_KEYS.put("TYPE_CAT", "typecat");
        MAPPED_KEYS.put("TYPE_SCHEM", "typeschem");
        MAPPED_KEYS.put("TYPE_NAME", "typename");
        MAPPED_KEYS.put("SELF_REFERENCING_COL_NAME", "selfreferencingcolname");
        MAPPED_KEYS.put("REF_GENERATION", "selfgeneration");
    }

    private final List<Map<String, Object>> rows = new ArrayList<>();

    /**
     * ResultSetGetTables constructor, initializes super class.
     *
     * @param statement                Statement Object.
     * @param graphSchemas          List of GraphSchema Objects.
     * @param resultSetInfoWithoutRows ResultSetInfoWithoutRows Object.
     */
    public ResultSetGetTables(final Statement statement,
                              final List<GraphSchema> graphSchemas,
                              final ResultSetInfoWithoutRows resultSetInfoWithoutRows) {
        super(statement, resultSetInfoWithoutRows.getColumns(), resultSetInfoWithoutRows.getRowCount());
        for (final GraphSchema graphSchema : graphSchemas) {
            // Add defaults, table name, and push into List.
            final Map<String, Object> map = new HashMap<>(MAPPED_KEYS);
            map.put(TABLE_NAME, nodeListToString(graphSchema.getLabels()));
            rows.add(map);
        }
    }

    /**
     * Function to sort nodes so that node sorting is consistent so that table names which are concatenated node labels
     * are also sorted.
     *
     * @param nodes List of nodes to sort and Stringify.
     * @return Return String joined list after sorting.
     */
    public static String nodeListToString(final List<String> nodes) {
        // Don't overly care how it is sorted as long as it is consistent.
        // Need to copy list in case it is an ImmutableList underneath.
        final List<String> sortedNodes = new ArrayList<>(nodes);
        java.util.Collections.sort(sortedNodes);
        return String.join(":", sortedNodes);
    }

    public static List<String> getColumns() {
        return ORDERED_COLUMNS;
    }

    @Override
    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        verifyOpen();
        final int index = getRowIndex();
        if ((index < 0) || (index >= rows.size())) {
            throw SqlError.createSQLException(LOGGER, SqlState.DATA_EXCEPTION, SqlError.INVALID_INDEX, index + 1,
                    rows.size());
        }
        if ((columnIndex <= 0) || (columnIndex > ORDERED_COLUMNS.size())) {
            throw SqlError
                    .createSQLException(LOGGER, SqlState.DATA_EXCEPTION, SqlError.INVALID_COLUMN_INDEX, columnIndex,
                            ORDERED_COLUMNS.size());
        }

        final String key = ORDERED_COLUMNS.get(columnIndex - 1);
        if (rows.get(index).containsKey(key)) {
            return rows.get(index).get(key);
        } else {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    protected void doClose() {
    }

    @Override
    protected int getDriverFetchSize() {
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {
    }
}
