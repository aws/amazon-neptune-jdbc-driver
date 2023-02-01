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

package software.aws.neptune.opencypher;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.opencypher.mock.MockOpenCypherDatabase;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class OpenCypherDatabaseMetadataTest {
    private static final String HOSTNAME = "localhost";
    private static final Properties PROPERTIES = new Properties();
    private static final Set<Set<String>> GET_TABLES_NODE_SET = ImmutableSet.of(
            ImmutableSet.of("Person"),
            ImmutableSet.of("Person", "Developer"),
            ImmutableSet.of("Person", "Human"),
            ImmutableSet.of("Person", "Developer", "Human"),
            ImmutableSet.of("Human", "Developer"),
            ImmutableSet.of("Dog"),
            ImmutableSet.of("Cat"));
    private static final Set<Set<String>> GET_TABLES_PERSON_NODE_SET = ImmutableSet.of(
            ImmutableSet.of("Person"),
            ImmutableSet.of("Person", "Developer"),
            ImmutableSet.of("Person", "Human"),
            ImmutableSet.of("Person", "Developer", "Human"));
    private static final String CREATE_NODES;
    private static final Set<String> GET_TABLES_NULL_SET = ImmutableSet.of(
            "TABLE_CAT", "TABLE_SCHEM", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
            "REF_GENERATION");
    private static MockOpenCypherDatabase database;
    private static java.sql.DatabaseMetaData databaseMetaData;

    static {
        final StringBuilder stringBuilder = new StringBuilder();
        GET_TABLES_NODE_SET.forEach(n -> stringBuilder.append(String.format(" CREATE (:%s {})", String.join(":", n))));
        CREATE_NODES = stringBuilder.replace(0, 1, "").toString();
    }

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() throws SQLException {
        database = MockOpenCypherDatabase.builder(HOSTNAME, OpenCypherDatabaseMetadataTest.class.getName()).build();
        PROPERTIES.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reverse default to None
        PROPERTIES.putIfAbsent(OpenCypherConnectionProperties.ENDPOINT_KEY,
                String.format("bolt://%s:%d", HOSTNAME, database.getPort()));
        final java.sql.Connection connection = new OpenCypherConnection(new OpenCypherConnectionProperties(PROPERTIES));
        final java.sql.Statement statement = connection.createStatement();
        statement.execute(CREATE_NODES);
        databaseMetaData = connection.getMetaData();
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        database.shutdown();
    }

    @Disabled
    @Test
    void testGetTables() throws SQLException {
        final java.sql.ResultSet resultSet =
                databaseMetaData.getTables(null, null, null, null);
        Assertions.assertTrue(resultSet.next());
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        final Set<Set<String>> expectedTables = new HashSet<>(GET_TABLES_NODE_SET);
        do {
            for (int i = 1; i <= columnCount; i++) {
                final String columnName = metaData.getColumnName(i);
                if (GET_TABLES_NULL_SET.contains(columnName)) {
                    Assertions.assertNull(resultSet.getString(i));
                } else if ("TABLE_NAME".equals(columnName)) {
                    final Set<String> labels = new HashSet<>(Arrays.asList(resultSet.getString(i).split(":")));
                    Assertions.assertTrue(expectedTables.contains(labels),
                            String.format("Table name set '%s' is not in the expected tables set.", labels.toString()));
                    expectedTables.remove(labels);
                } else if ("TABLE_TYPE".equals(columnName)) {
                    Assertions.assertEquals("TABLE", resultSet.getString(i));
                } else if ("REMARKS".equals(columnName)) {
                    Assertions.assertEquals("", resultSet.getString(i));
                } else {
                    Assertions.fail(
                            String.format("Unexpected column name '%s' encountered for table metadata.", columnName));
                }
            }
        } while (resultSet.next());
    }

    @Disabled
    @Test
    void testGetTablesPersonOnly() throws SQLException {
        final java.sql.ResultSet resultSet =
                databaseMetaData.getTables(null, null, "Person", null);
        Assertions.assertTrue(resultSet.next());
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        final Set<Set<String>> expectedTables = new HashSet<>(GET_TABLES_PERSON_NODE_SET);
        do {
            for (int i = 1; i <= columnCount; i++) {
                final String columnName = metaData.getColumnName(i);
                if (GET_TABLES_NULL_SET.contains(columnName)) {
                    Assertions.assertNull(resultSet.getString(i));
                } else if ("TABLE_NAME".equals(columnName)) {
                    final Set<String> labels = new HashSet<>(Arrays.asList(resultSet.getString(i).split(":")));
                    Assertions.assertTrue(expectedTables.contains(labels),
                            String.format("Table name set '%s' is not in the expected tables set.", labels.toString()));
                    expectedTables.remove(labels);
                } else if ("TABLE_TYPE".equals(columnName)) {
                    Assertions.assertEquals("TABLE", resultSet.getString(i));
                } else if ("REMARKS".equals(columnName)) {
                    Assertions.assertEquals("", resultSet.getString(i));
                } else {
                    Assertions.fail(
                            String.format("Unexpected column name '%s' encountered for table metadata.", columnName));
                }
            }
        } while (resultSet.next());
    }

    @Test
    void testGetCatalogs() throws SQLException {
        final java.sql.ResultSet resultSet = databaseMetaData.getCatalogs();
        // Catalog is not currently supported
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    void testGetSchemas() throws SQLException {
        final java.sql.ResultSet resultSet = databaseMetaData.getSchemas();
        Assertions.assertTrue(resultSet.next());
        // TODO: Fix this to check the value later.
    }

    @Test
    void testGetTableTypes() throws SQLException {
        final java.sql.ResultSet resultSet = databaseMetaData.getTableTypes();
        Assertions.assertTrue(resultSet.next());
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        Assertions.assertEquals(1, columnCount);
        Assertions.assertEquals("TABLE", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    void testGetTypeInfo() throws SQLException {
        final java.sql.ResultSet resultSet = databaseMetaData.getTypeInfo();
        Assertions.assertTrue(resultSet.next());
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        Assertions.assertEquals(18, columnCount);
        Assertions.assertEquals("BOOLEAN", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
    }
}
