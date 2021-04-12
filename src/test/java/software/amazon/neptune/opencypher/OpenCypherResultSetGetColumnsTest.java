/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.neptune.opencypher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.neptune.opencypher.resultset.OpenCypherResultSet;
import software.amazon.neptune.opencypher.resultset.OpenCypherResultSetGetColumns;
import software.amazon.neptune.opencypher.resultset.OpenCypherResultSetGetTables;
import software.amazon.neptune.opencypher.utilities.OpenCypherGetColumnUtilities;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OpenCypherResultSetGetColumnsTest {
    protected static final Properties PROPERTIES = new Properties();
    private static final Map<String, Map<String, Map<String, Object>>> COLUMNS = new HashMap<>();
    private static java.sql.Statement statement;

    static {
        COLUMNS.put(OpenCypherResultSetGetTables.nodeListToString(ImmutableList.of("A", "B", "C")),
                ImmutableMap.of(
                        "name", ImmutableMap.of(
                                "dataType", "String",
                                "isMultiValue", false,
                                "isNullable", false),
                        "email", ImmutableMap.of(
                                "dataType", "String",
                                "isMultiValue", false,
                                "isNullable", false)));
        COLUMNS.put(OpenCypherResultSetGetTables.nodeListToString(ImmutableList.of("A", "B", "C", "D")),
                ImmutableMap.of(
                        "age", ImmutableMap.of(
                                "dataType", "Integer",
                                "isMultiValue", false,
                                "isNullable", false),
                        "email", ImmutableMap.of(
                                "dataType", "String",
                                "isMultiValue", false,
                                "isNullable", false)));
    }

    /**
     * Function to initialize java.sql.Statement for use in tests.
     *
     * @throws SQLException Thrown if initialization fails.
     */
    @BeforeAll
    public static void initialize() throws SQLException {
        PROPERTIES.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reverse default to None
        // Make up fake endpoint since we aren't actually connection.
        PROPERTIES.putIfAbsent(OpenCypherConnectionProperties.ENDPOINT_KEY, String.format("bolt://%s:%d", "localhost", 123));
        final java.sql.Connection connection = new OpenCypherConnection(new OpenCypherConnectionProperties(PROPERTIES));
        statement = connection.createStatement();
    }

    @Test
    void generateOpenCypherResultSetGetColumnsManuallyTest() throws Exception {
        final ResultSet resultSet =
                new OpenCypherResultSetGetColumns(statement, OpenCypherGetColumnUtilities.NODE_COLUMN_INFOS,
                        new OpenCypherResultSet.ResultSetInfoWithoutRows(null, null,
                                OpenCypherGetColumnUtilities.NODE_COLUMN_INFOS.size(),
                                OpenCypherResultSetGetColumns.getColumns()));
        final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        verifyResultSetMetadata(resultSetMetaData);
        verifyResultSet(resultSet);
    }

    void verifyResultSetMetadata(final ResultSetMetaData metadata) throws SQLException {
        Assertions.assertEquals(OpenCypherGetColumnUtilities.COLUMN_NAMES.size(), metadata.getColumnCount());
        for (int i = 1; i <= OpenCypherGetColumnUtilities.COLUMN_NAMES.size(); i++) {
            Assertions.assertEquals(OpenCypherGetColumnUtilities.COLUMN_NAMES.get(i - 1), metadata.getColumnName(i));
        }
    }

    void verifyResultSet(final ResultSet resultSet) throws SQLException {
        Assertions.assertTrue(resultSet.next());
        final int i = 1;
        do {
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertNull(resultSet.getString(2));
            final String tableName = resultSet.getString(3);
            Assertions.assertTrue(COLUMNS.containsKey(tableName));

            final Map<String, Map<String, Object>> columnNameInfo = COLUMNS.get(tableName);
            final String columnName = resultSet.getString(4);
            Assertions.assertTrue(columnNameInfo.containsKey(columnName));
            final Map<String, Object> columnInfo = columnNameInfo.get(columnName);

            // TODO: validate JDBC type. (idx 5)

            Assertions.assertEquals(columnInfo.get("dataType"), resultSet.getString(6));

            // TODO: Determine what to do with column size. (idx 7)

            Assertions.assertNull(resultSet.getString(8));
            Assertions.assertNull(resultSet.getString(9));
            Assertions.assertEquals(10, resultSet.getInt(10));
            Assertions.assertEquals((Boolean) columnInfo.get("isNullable") ? DatabaseMetaData.columnNullable :
                    DatabaseMetaData.columnNoNulls, resultSet.getInt(11));
            Assertions.assertNull(resultSet.getString(12));
            Assertions.assertNull(resultSet.getString(13));
            Assertions.assertNull(resultSet.getString(14));
            Assertions.assertNull(resultSet.getString(15));
            if ("String".equals(resultSet.getString(6))) {
                Assertions.assertEquals(Integer.MAX_VALUE, resultSet.getInt(16));
            } else {
                Assertions.assertEquals(0, resultSet.getInt(16));
                Assertions.assertTrue(resultSet.wasNull());
            }
            // Assertions.assertEquals(i++, resultSet.getInt(17)); TODO: Collect and make sure all come out.
            Assertions.assertEquals((Boolean) columnInfo.get("isNullable") ? "YES" : "NO", resultSet.getString(18));
            Assertions.assertNull(resultSet.getString(19));
            Assertions.assertNull(resultSet.getString(20));
            Assertions.assertNull(resultSet.getString(21));
            Assertions.assertNull(resultSet.getString(22));
            Assertions.assertEquals("NO", resultSet.getString(23));
            Assertions.assertEquals("NO", resultSet.getString(24));

        } while (resultSet.next());
    }
}
