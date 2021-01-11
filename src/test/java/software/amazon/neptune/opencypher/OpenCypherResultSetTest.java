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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.neptune.NeptuneConstants;
import software.amazon.neptune.opencypher.mock.MockOpenCypherDatabase;
import java.sql.SQLException;
import java.util.Properties;

public class OpenCypherResultSetTest {
    private static final String HOSTNAME = "localhost";
    private static final Properties PROPERTIES = new Properties();
    private static MockOpenCypherDatabase database;
    private java.sql.Statement statement;

    /**
     * Function to get a random available port and initiaize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() {
        database = MockOpenCypherDatabase.builder(HOSTNAME, OpenCypherResultSetMetadataTest.class.getName()).build();
        PROPERTIES.putIfAbsent(NeptuneConstants.ENDPOINT, String.format("bolt://%s:%d", HOSTNAME, database.getPort()));
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        database.shutdown();
    }

    @BeforeEach
    void initialize() throws SQLException {
        final java.sql.Connection connection = new OpenCypherConnection(PROPERTIES);
        statement = connection.createStatement();
    }

    // Primitive types.
    @Test
    void testNullType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN null as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertFalse(resultSet.getBoolean(0));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals((byte) 0, resultSet.getByte(0));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals((short) 0, resultSet.getShort(0));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals(0, resultSet.getInt(0));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals(0, resultSet.getLong(0));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals(0.0f, resultSet.getFloat(0));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals(0.0, resultSet.getDouble(0));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertNull(resultSet.getString(0));
        Assertions.assertTrue(resultSet.wasNull());
    }

    @Test
    void testBooleanType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN true as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean(0));
        Assertions.assertEquals(((Boolean) true).toString(), resultSet.getString(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(0));
    }

    @Test
    void testSimpleNumericType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN 1 as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(1L, resultSet.getLong(0));
        Assertions.assertEquals(1, resultSet.getInt(0));
        Assertions.assertEquals((short) 1, resultSet.getShort(0));
        Assertions.assertEquals((byte) 1, resultSet.getByte(0));
        Assertions.assertEquals(((Integer) 1).toString(), resultSet.getString(0));
        Assertions.assertTrue(resultSet.getBoolean(0));
    }

    @Test
    void testLargeNumericType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN 4147483647 as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(4147483647L, resultSet.getLong(0));
        Assertions.assertEquals((int) 4147483647L, resultSet.getInt(0));
        Assertions.assertEquals((short) 4147483647L, resultSet.getShort(0));
        Assertions.assertEquals((byte) 4147483647L, resultSet.getByte(0));
        Assertions.assertEquals(((Long) 4147483647L).toString(), resultSet.getString(0));
        Assertions.assertTrue(resultSet.getBoolean(0));
    }

    @Test
    void testFloatingPointType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN 1.0 as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(1.0f, resultSet.getFloat(0));
        Assertions.assertEquals(1.0, resultSet.getDouble(0));
        Assertions.assertEquals((byte) 1.0, resultSet.getByte(0));
        Assertions.assertEquals((short) 1.0, resultSet.getShort(0));
        Assertions.assertEquals((int) 1.0, resultSet.getInt(0));
        Assertions.assertEquals((long) 1.0, resultSet.getLong(0));
        Assertions.assertEquals(((Double) 1.0).toString(), resultSet.getString(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(0));
    }

    @Test
    void testStringType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN 'hello' as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(resultSet.getString(0), "hello");
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(0));
    }

    @Test
    void testNumericStringType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN '1.0' as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(resultSet.getString(0), "1.0");
        Assertions.assertEquals(resultSet.getString(0), ((Double) 1.0).toString());
        Assertions.assertEquals(resultSet.getFloat(0), 1.0f);
        Assertions.assertEquals(resultSet.getDouble(0), 1.0);
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(0));
    }

    // Composite types
    @Test
    void testArrayType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN ['hello', 'world'] as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("[hello, world]", resultSet.getString(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(0));
    }

    @Test
    void testMapType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN ({hello:'world'}) as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("{hello=world}", resultSet.getString(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(0));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(0));
    }

    // TODO: Date and Time types / Graph types.
}
