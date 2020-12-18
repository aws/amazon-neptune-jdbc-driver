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

package software.amazon.neptune.opencypher;

import lombok.AllArgsConstructor;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.ResultSetMetaData;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: When implementing next steps, abstract minimal information into an interface and implement it.
/**
 * OpenCypher implementation of ResultSetMetadata.
 */
@AllArgsConstructor
public class OpenCypherResultSetMetadata extends ResultSetMetaData implements java.sql.ResultSetMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherResultSetMetadata.class);
    private final List<String> columns;
    private final List<Record> rows;
    private static final Map<Type, Integer> BOLT_TO_JDBC_TYPE_MAP = new HashMap<>();
    private static final Map<Type, Class> BOLT_TO_JAVA_TYPE_MAP = new HashMap<>();
    static {
        // Bolt->JDBC mapping.
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.ANY(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BOOLEAN(), Types.BIT);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BYTES(), Types.VARCHAR); // TODO: Revisit
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.STRING(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NUMBER(), Types.DOUBLE); // TODO double check this is floating point
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.INTEGER(),Types.BIGINT);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.FLOAT(), Types.DOUBLE);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LIST(), Types.VARCHAR); // TODO Revisit list and map support.
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.MAP(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NODE(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.RELATIONSHIP(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.PATH(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.POINT(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE(), Types.DATE); // TODO: Look into datetime more closely.
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.TIME(), Types.TIME); // TODO: Scope only says dates, do we need time/etc?
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_TIME(), Types.TIME);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME(), Types.TIMESTAMP);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE_TIME(), Types.TIMESTAMP);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DURATION(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NULL(), Types.NULL);

        // Bolt->Java mapping.
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.ANY(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BOOLEAN(), Boolean.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BYTES(), String.class); // TODO: Revisit
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.STRING(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NUMBER(), Double.class); // TODO double check this is floating point
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.INTEGER(),Long.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.FLOAT(), Double.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LIST(), String.class); // TODO Revisit list and map support.
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.MAP(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NODE(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.RELATIONSHIP(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.PATH(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.POINT(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE(), java.sql.Date.class); // TODO: Look into datetime more closely.
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.TIME(), java.sql.Time.class); // TODO: Scope only says dates, do we need time/etc?
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_TIME(), java.sql.Time.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME(), java.sql.Timestamp.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE_TIME(), java.sql.Timestamp.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DURATION(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NULL(), Object.class);
    }

    /**
     * Verify if the given column index is valid.
     * @param column the 1-based column index.
     * @throws SQLException if the column index is not valid for this result.
     */
    private void verifyColumnIndex(final int column) throws SQLException {
        if ((column <= 0) || (column > columns.size())) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.INVALID_INDEX,
                    column,
                    columns.size());
        }
    }

    /**
     * Get Bolt type of a given column.
     * @param column the 1-based column index.
     * @return Bolt Type Object for column.
     */
    private Type getColumnBoltType(final int column) {
        // TODO: Loop rows to find common type and cache it.
        return rows.get(0).values().get(column - 1).type();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
    }

    @Override
    public int getColumnDisplaySize(final int column) throws SQLException {
        verifyColumnIndex(column);

        final int type = getColumnType(column);
        if (type == Types.BIT) {
            return 1;
        } else if (type == Types.VARCHAR) {
            return 0;
        } else if (type == Types.DOUBLE) {
            return 25;
        } else if (type == Types.DATE) {
            return 24;
        } else if (type == Types.TIME) {
            return 24;
        } else if (type == Types.TIMESTAMP) {
            return 24;
        } else if (type == Types.BIGINT) {
            return 20;
        } else if (type == Types.NULL) {
            return 0;
        } else {
            LOGGER.warn(String.format("Unsupported data type for getColumnDisplaySize(%d).", type));
            return 0;
        }
    }

    @Override
    public int getPrecision(final int column) throws SQLException {
        verifyColumnIndex(column);

        final int type = getColumnType(column);
        if (type == Types.BIT) {
            return 1;
        } else if (type == Types.VARCHAR) {
            return 256;
        } else if (type == Types.DOUBLE) {
            return 15;
        } else if (type == Types.DATE) {
            return 24;
        } else if (type == Types.TIME) {
            return 24;
        } else if (type == Types.TIMESTAMP) {
            return 24;
        } else if (type == Types.BIGINT) {
            return 19;
        } else if (type == Types.NULL) {
            return 0;
        } else {
            LOGGER.warn(String.format("Unsupported data type for getPrecision(%d).", type));
            return 0;
        }
    }

    @Override
    public int getScale(final int column) throws SQLException {
        verifyColumnIndex(column);

        // 15.9 significant digits after decimal, truncate to 15.
        return (getColumnType(column) == Types.DOUBLE) ? 15 : 0;
    }

    @Override
    public boolean isAutoIncrement(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Concept doesn't exist.
        return false;
    }

    @Override
    public boolean isCaseSensitive(final int column) throws SQLException {
        verifyColumnIndex(column);
        return (getColumnClassName(column).equals(String.class.getName()));
    }

    @Override
    public boolean isSearchable(final int column) throws SQLException {
        verifyColumnIndex(column);

        // We don't support WHERE clauses in the typical SQL way, so not searchable.
        return false;
    }

    @Override
    public boolean isCurrency(final int column) throws SQLException {
        verifyColumnIndex(column);

        // If it is currency, there's no way to know.
        return false;
    }

    @Override
    public int isNullable(final int column) throws SQLException {
        verifyColumnIndex(column);
        return java.sql.ResultSetMetaData.columnNullableUnknown;
    }

    @Override
    public boolean isSigned(final int column) throws SQLException {
        verifyColumnIndex(column);

        final int type = getColumnType(column);
        return ((type == Types.BIGINT) || (type == Types.DOUBLE));
    }

    @Override
    public String getColumnLabel(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Label is same as name.
        return getColumnName(column);
    }

    @Override
    public String getColumnName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columns.get(column - 1);
    }

    @Override
    public int getColumnType(final int column) throws SQLException {
        verifyColumnIndex(column);
        return BOLT_TO_JDBC_TYPE_MAP.get(getColumnBoltType(column));
    }

    @Override
    public String getColumnTypeName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return getColumnBoltType(column).name();
    }

    @Override
    public String getColumnClassName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return BOLT_TO_JAVA_TYPE_MAP.get(getColumnBoltType(column)).getName();
    }

    @Override
    public boolean isReadOnly(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Read only driver.
        return true;
    }

    @Override
    public boolean isWritable(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Read only driver.
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Read only driver.
        return false;
    }

    @Override
    public String getTableName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Doesn't have the concept of tables.
        return "";
    }

    @Override
    public String getSchemaName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Doesn't have the concept of schema.
        return "";
    }

    @Override
    public String getCatalogName(final int column) throws SQLException {
        verifyColumnIndex(column);

        // Doesn't have the concept of catalog.
        return "";
    }
}
