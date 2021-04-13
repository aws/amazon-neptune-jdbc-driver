/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License testIs located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file testIs distributed
 * on an "AS testIs" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.neptune.opencypher;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import software.amazon.neptune.opencypher.mock.MockOpenCypherDatabase;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Properties;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.DATE;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.DATE_TIME;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.DURATION;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.LOCAL_DATE_TIME;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.LOCAL_TIME;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.NODE;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.NON_EMPTY_LIST;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.NON_EMPTY_MAP;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.NON_EMPTY_STRING;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.NULL;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.PATH;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.POINT_2D;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.POINT_3D;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.POS_DOUBLE;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.POS_INTEGER;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.RELATIONSHIP;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.TIME;
import static software.amazon.neptune.opencypher.mock.OpenCypherQueryLiterals.TRUE;

public class OpenCypherResultSetMetadataTest {
    private static final String HOSTNAME = "localhost";
    private static final Properties PROPERTIES = new Properties();
    private static final List<MetadataTestHelper> METADATA_TEST_HELPER = ImmutableList.of(
            new MetadataTestHelper(NULL,
                    0, 0, 0, false, false, Types.NULL, Object.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.NULL().name()),
            new MetadataTestHelper(POS_INTEGER,
                    20, 19, 0, false, true, java.sql.Types.BIGINT, Long.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.INTEGER().name()),
            new MetadataTestHelper(NON_EMPTY_STRING,
                    0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.STRING().name()),
            new MetadataTestHelper(TRUE,
                    1, 1, 0, false, false, java.sql.Types.BIT, Boolean.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.BOOLEAN().name()),
            new MetadataTestHelper(POS_DOUBLE,
                    25, 15, 15, false, true, java.sql.Types.DOUBLE, Double.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.FLOAT().name()),
            new MetadataTestHelper(NON_EMPTY_MAP,
                    0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.MAP().name()),
            new MetadataTestHelper(NON_EMPTY_LIST,
                    0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.LIST().name()),
            new MetadataTestHelper(DATE,
                    24, 24, 0, false, false, java.sql.Types.DATE, java.sql.Date.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.DATE().name()),
            new MetadataTestHelper(TIME,
                    24, 24, 0, false, false, java.sql.Types.TIME, java.sql.Time.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.TIME().name()),
            new MetadataTestHelper(LOCAL_TIME,
                    24, 24, 0, false, false, java.sql.Types.TIME, java.sql.Time.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.LOCAL_TIME().name()),
            new MetadataTestHelper(DATE_TIME,
                    24, 24, 0, false, false, java.sql.Types.TIMESTAMP, java.sql.Timestamp.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.DATE_TIME().name()),
            new MetadataTestHelper(LOCAL_DATE_TIME,
                    24, 24, 0, false, false, java.sql.Types.TIMESTAMP, java.sql.Timestamp.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME().name()),
            new MetadataTestHelper(DURATION,
                    0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.DURATION().name()),
            new MetadataTestHelper(NODE,
                    0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.NODE().name()),
            new MetadataTestHelper(RELATIONSHIP,
                    0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.RELATIONSHIP().name()),
            new MetadataTestHelper(PATH,
                    0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.PATH().name()),
            new MetadataTestHelper(POINT_2D,
                    0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.POINT().name()),
            new MetadataTestHelper(POINT_3D,
                    0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                    InternalTypeSystem.TYPE_SYSTEM.POINT().name())
    );
    private static MockOpenCypherDatabase database;
    private static java.sql.Statement statement;

    /**
     * Function to get a random available port and initiaize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() throws SQLException {
        database = MockOpenCypherDatabase.builder(HOSTNAME, OpenCypherResultSetMetadataTest.class.getName()).build();
        PROPERTIES.putIfAbsent(OpenCypherConnectionProperties.ENDPOINT_KEY,
                String.format("bolt://%s:%d", HOSTNAME, database.getPort()));
        final java.sql.Connection connection = new OpenCypherConnection(new OpenCypherConnectionProperties(PROPERTIES));
        statement = connection.createStatement();
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        database.shutdown();
    }

    ResultSetMetaData getResultSetMetaData(final String query) throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(query);
        return resultSet.getMetaData();
    }

    @Test
    void testGetColumnCount() throws SQLException {
        Assertions.assertEquals(1, getResultSetMetaData("MATCH (n) RETURN n").getColumnCount());
        Assertions.assertEquals(1, getResultSetMetaData("RETURN true AS l1").getColumnCount());
        Assertions.assertEquals(3, getResultSetMetaData("MATCH (n) RETURN n.foo, n.bar, n.baz").getColumnCount());
    }

    @Test
    void testGetColumnDisplaySize() throws SQLException {
        for (final MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getDisplaySize(),
                    getResultSetMetaData(helper.getQuery()).getColumnDisplaySize(1), "For query: " + helper.getQuery());
        }
    }

    @Test
    void testGetPrecision() throws SQLException {
        for (final MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getPrecision(), getResultSetMetaData(helper.getQuery()).getPrecision(1),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testGetScale() throws SQLException {
        for (final MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getScale(), getResultSetMetaData(helper.getQuery()).getScale(1),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testIsAutoIncrement() throws SQLException {
        Assertions.assertFalse(getResultSetMetaData("RETURN 'foo' as n").isAutoIncrement(1));
        Assertions.assertFalse(getResultSetMetaData("RETURN 1 as n").isAutoIncrement(1));
    }

    @Test
    void testIsCaseSensitive() throws SQLException {
        Assertions.assertTrue(getResultSetMetaData("RETURN 'foo' as n").isCaseSensitive(1));
        Assertions.assertFalse(getResultSetMetaData("RETURN 1 as n").isCaseSensitive(1));
    }

    @Test
    void testIsSearchable() throws SQLException {
        Assertions.assertFalse(getResultSetMetaData("RETURN 'foo' as n").isSearchable(1));
    }

    @Test
    void testIsCurrency() throws SQLException {
        Assertions.assertFalse(getResultSetMetaData("RETURN 'foo' as n").isCurrency(1));
        Assertions.assertFalse(getResultSetMetaData("RETURN 1 as n").isCurrency(1));
    }

    @Test
    void testIsNullable() throws SQLException {
        Assertions.assertEquals(ResultSetMetaData.columnNullableUnknown,
                getResultSetMetaData("RETURN 'foo' as n").isNullable(1));
        Assertions.assertEquals(ResultSetMetaData.columnNullableUnknown,
                getResultSetMetaData("RETURN 1 as n").isNullable(1));
        Assertions.assertEquals(ResultSetMetaData.columnNullableUnknown,
                getResultSetMetaData("RETURN true as n").isNullable(1));
    }

    @Test
    void testIsSigned() throws SQLException {
        for (final MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.isSigned(), getResultSetMetaData(helper.getQuery()).isSigned(1),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testGetColumnLabel() throws SQLException {
        Assertions.assertEquals("n", getResultSetMetaData("Return 1 as n").getColumnName(1));
    }

    @Test
    void testGetColumnName() throws SQLException {
        Assertions.assertEquals("n", getResultSetMetaData("Return 1 as n").getColumnName(1));
    }

    @Test
    void testGetColumnType() throws SQLException {
        for (final MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getJdbcType(), getResultSetMetaData(helper.getQuery()).getColumnType(1),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testGetColumnTypeName() throws SQLException {
        for (final MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getInternalColumnClassName(),
                    getResultSetMetaData(helper.getQuery()).getColumnTypeName(1), "For query: " + helper.getQuery());
        }
    }

    @Test
    void testGetColumnClassName() throws SQLException {
        for (final MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getColumnClassName(),
                    getResultSetMetaData(helper.getQuery()).getColumnClassName(1), "For query: " + helper.getQuery());
        }
    }

    @Test
    void testIsReadOnly() throws SQLException {
        Assertions.assertTrue(getResultSetMetaData("RETURN 'foo' as n").isReadOnly(1));
    }

    @Test
    void testIsWritable() throws SQLException {
        Assertions.assertFalse(getResultSetMetaData("RETURN 'foo' as n").isWritable(1));
    }

    @Test
    void testIsDefinitelyWritable() throws SQLException {
        Assertions.assertFalse(getResultSetMetaData("RETURN 'foo' as n").isDefinitelyWritable(1));
    }

    @Test
    void testGetTableName() throws SQLException {
        Assertions.assertEquals("", getResultSetMetaData("RETURN 'foo' as n").getTableName(1));
    }

    @Test
    void testGetSchemaName() throws SQLException {
        Assertions.assertEquals("", getResultSetMetaData("RETURN 'foo' as n").getSchemaName(1));
    }

    @Test
    void testGetCatalogName() throws SQLException {
        Assertions.assertEquals("", getResultSetMetaData("RETURN 'foo' as n").getCatalogName(1));
    }

    @AllArgsConstructor
    @Getter
    static
    class MetadataTestHelper {
        private final String query;
        private final int displaySize;
        private final int precision;
        private final int scale;
        private final boolean caseSensitive;
        private final boolean signed;
        private final int jdbcType;
        private final String columnClassName;
        private final String internalColumnClassName;
    }
}
