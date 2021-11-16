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

package software.aws.neptune.common.gremlindatamodel.resultset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinProperty;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinTableBase;
import software.aws.neptune.common.ResultSetInfoWithoutRows;
import software.aws.neptune.jdbc.ResultSet;
import software.aws.neptune.jdbc.utilities.JavaToJdbcTypeConverter;
import software.aws.neptune.jdbc.utilities.JdbcType;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Base ResultSet for getColumns.
 */
public abstract class ResultSetGetColumns extends ResultSet
        implements java.sql.ResultSet {
    public static final Map<String, Class<?>> GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultSetGetColumns.class);
    /**
     * TABLE_CAT String => table catalog (may be null)
     * TABLE_SCHEM String => table schema (may be null)
     * TABLE_NAME String => table name
     * COLUMN_NAME String => column name
     * DATA_TYPE int => SQL type from java.sql.Types
     * TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
     * COLUMN_SIZE int => column size.
     * BUFFER_LENGTH is not used.
     * DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
     * NUM_PREC_RADIX int => Radix (typically either 10 or 2)
     * NULLABLE int => is NULL allowed.
     * columnNoNulls - might not allow NULL values
     * columnNullable - definitely allows NULL values
     * columnNullableUnknown - nullability unknown
     * REMARKS String => comment describing column (may be null)
     * COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
     * SQL_DATA_TYPE int => unused
     * SQL_DATETIME_SUB int => unused
     * CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
     * ORDINAL_POSITION int => index of column in table (starting at 1)
     * IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
     * YES --- if the column can include NULLs
     * NO --- if the column cannot include NULLs
     * empty string --- if the nullability for the column is unknown
     * SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
     * SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
     * SCOPE_TABLE String => table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF)
     * SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
     * IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
     * YES --- if the column is auto incremented
     * NO --- if the column is not auto incremented
     * empty string --- if it cannot be determined whether the column is auto incremented
     * IS_GENERATEDCOLUMN String => Indicates whether this is a generated column
     * YES --- if this a generated column
     * NO --- if this not a generated column
     * empty string --- if it cannot be determined whether this is a generated column
     */
    private static final Map<String, Object> CONVERSION_MAP = new HashMap<>();
    private static final List<String> ORDERED_COLUMNS = new ArrayList<>();

    static {
        CONVERSION_MAP.put("TABLE_CAT", null);
        CONVERSION_MAP.put("TABLE_SCHEM", "gremlin");
        CONVERSION_MAP.put("BUFFER_LENGTH", null); // null
        CONVERSION_MAP.put("NULLABLE", DatabaseMetaData.columnNullable);
        CONVERSION_MAP.put("REMARKS", null); // null
        CONVERSION_MAP.put("SQL_DATA_TYPE", null); // null
        CONVERSION_MAP.put("SQL_DATETIME_SUB", null); // null
        CONVERSION_MAP.put("IS_NULLABLE", "YES");
        CONVERSION_MAP.put("SCOPE_CATALOG", null); // null
        CONVERSION_MAP.put("SCOPE_SCHEMA", null); // null
        CONVERSION_MAP.put("SCOPE_TABLE", null); // null
        CONVERSION_MAP.put("SOURCE_DATA_TYPE", null); // null
        CONVERSION_MAP.put("IS_AUTOINCREMENT", "NO");
        CONVERSION_MAP.put("IS_GENERATEDCOLUMN", "NO");
        CONVERSION_MAP.put("COLUMN_DEF", null);

        ORDERED_COLUMNS.add("TABLE_CAT");
        ORDERED_COLUMNS.add("TABLE_SCHEM");
        ORDERED_COLUMNS.add("TABLE_NAME");
        ORDERED_COLUMNS.add("COLUMN_NAME");
        ORDERED_COLUMNS.add("DATA_TYPE");
        ORDERED_COLUMNS.add("TYPE_NAME");
        ORDERED_COLUMNS.add("COLUMN_SIZE");
        ORDERED_COLUMNS.add("BUFFER_LENGTH");
        ORDERED_COLUMNS.add("DECIMAL_DIGITS");
        ORDERED_COLUMNS.add("NUM_PREC_RADIX");
        ORDERED_COLUMNS.add("NULLABLE");
        ORDERED_COLUMNS.add("REMARKS");
        ORDERED_COLUMNS.add("COLUMN_DEF");
        ORDERED_COLUMNS.add("SQL_DATA_TYPE");
        ORDERED_COLUMNS.add("SQL_DATETIME_SUB");
        ORDERED_COLUMNS.add("CHAR_OCTET_LENGTH");
        ORDERED_COLUMNS.add("ORDINAL_POSITION");
        ORDERED_COLUMNS.add("IS_NULLABLE");
        ORDERED_COLUMNS.add("SCOPE_CATALOG");
        ORDERED_COLUMNS.add("SCOPE_SCHEMA");
        ORDERED_COLUMNS.add("SCOPE_TABLE");
        ORDERED_COLUMNS.add("SOURCE_DATA_TYPE");
        ORDERED_COLUMNS.add("IS_AUTOINCREMENT");
        ORDERED_COLUMNS.add("IS_GENERATEDCOLUMN");

        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Byte", Byte.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Short", Short.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Integer", Integer.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Boolean", Boolean.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Long", Long.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Float", Float.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Double", Double.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("String", String.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Date", Date.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Time", Time.class);
    }

    private final List<Map<String, Object>> rows = new ArrayList<>();
    private boolean wasNull = false;

    /**
     * ResultSetGetColumns constructor, initializes super class.
     *
     * @param statement                Statement Object.
     * @param gremlinSchema            GremlinSchema Object.
     * @param resultSetInfoWithoutRows ResultSetInfoWithoutRows Object.
     */
    public ResultSetGetColumns(final Statement statement, final GremlinSchema gremlinSchema,
                               final ResultSetInfoWithoutRows resultSetInfoWithoutRows)
            throws SQLException {
        super(statement, resultSetInfoWithoutRows.getColumns(), resultSetInfoWithoutRows.getRowCount());
        for (final GremlinTableBase gremlinTableBase : gremlinSchema.getAllTables()) {
            int i = 1;
            for (final Map.Entry<String, GremlinProperty> property : gremlinTableBase.getColumns().entrySet()) {
                // Add defaults.
                final Map<String, Object> map = new HashMap<>(CONVERSION_MAP);

                // Set table name.
                map.put("TABLE_NAME", gremlinTableBase.getLabel());

                // Get column type.
                final String dataType = property.getValue().getType();
                map.put("TYPE_NAME", dataType);
                final Optional<? extends Class<?>> javaClassOptional =
                        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.
                                entrySet().stream().
                                filter(d -> d.getKey().equalsIgnoreCase(dataType)).
                                map(Map.Entry::getValue).
                                findFirst();
                final Class<?> javaClass = javaClassOptional.isPresent() ? javaClassOptional.get() : String.class;
                map.put("CHAR_OCTET_LENGTH", (javaClass == String.class) ? Integer.MAX_VALUE : null);
                final int jdbcType = JavaToJdbcTypeConverter.CLASS_TO_JDBC_ORDINAL
                        .getOrDefault(javaClass, JdbcType.VARCHAR.getJdbcCode());
                map.put("DATA_TYPE", jdbcType);
                map.put("SQL_DATA_TYPE", jdbcType);

                map.put("COLUMN_NAME", property.getKey());
                map.put("NULLABLE", DatabaseMetaData.columnNullable);
                map.put("IS_NULLABLE", "YES");

                // TODO: These need to be verified for Tableau.
                map.put("DECIMAL_DIGITS", null);
                map.put("NUM_PREC_RADIX", 10);
                map.put("ORDINAL_POSITION", i++);
                // TODO AN-839: Fix COLUMN_SIZE.
                map.put("COLUMN_SIZE", 10);

                if (!map.keySet().equals(new HashSet<>(ORDERED_COLUMNS))) {
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.DATA_TYPE_TRANSFORM_VIOLATION,
                            SqlError.UNSUPPORTED_TYPE, map.keySet().toString());
                }

                rows.add(map);
            }
        }
    }

    public static List<String> getColumns() {
        return ORDERED_COLUMNS;
    }

    @Override
    protected Object getConvertedValue(final int columnIndex) throws SQLException {
        verifyOpen();
        final int index = getRowIndex();
        if ((index < 0) || (index >= rows.size())) {
            throw SqlError
                    .createSQLException(LOGGER, SqlState.DATA_EXCEPTION, SqlError.INVALID_INDEX, getRowIndex() + 1,
                            rows.size());
        }
        if ((columnIndex <= 0) || (columnIndex > ORDERED_COLUMNS.size())) {
            throw SqlError
                    .createSQLException(LOGGER, SqlState.DATA_EXCEPTION, SqlError.INVALID_COLUMN_INDEX, columnIndex,
                            ORDERED_COLUMNS.size());
        }

        final String key = ORDERED_COLUMNS.get(columnIndex - 1);
        if (rows.get(index).containsKey(key)) {
            final Object data = rows.get(index).get(key);
            wasNull = (data == null);
            return data;
        } else {
            throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    protected void doClose() throws SQLException {
    }

    @Override
    protected int getDriverFetchSize() throws SQLException {
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {
    }
}
