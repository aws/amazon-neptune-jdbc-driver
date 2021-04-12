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
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.neptune.opencypher.mock.MockOpenCypherDatabase;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Properties;

public class OpenCypherResultSetTest {
    private static final String HOSTNAME = "localhost";
    private static final Properties PROPERTIES = new Properties();
    private static MockOpenCypherDatabase database;
    private static java.sql.Statement statement;

    /**
     * Function to get a random available port and initiaize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() throws SQLException {
        database = MockOpenCypherDatabase.builder(HOSTNAME, OpenCypherResultSetTest.class.getName()).build();
        PROPERTIES.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reverse default to None
        PROPERTIES.putIfAbsent(OpenCypherConnectionProperties.ENDPOINT_KEY, String.format("bolt://%s:%d", HOSTNAME, database.getPort()));
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

    @BeforeEach
    void initialize() throws SQLException {
    }

    // Primitive types.
    @Test
    void testNullType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN null as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertFalse(resultSet.getBoolean(1));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals((byte) 0, resultSet.getByte(1));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals((short) 0, resultSet.getShort(1));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals(0, resultSet.getInt(1));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals(0, resultSet.getLong(1));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals(0.0f, resultSet.getFloat(1));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertEquals(0.0, resultSet.getDouble(1));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertNull(resultSet.getString(1));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertNull(resultSet.getDate(1));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertNull(resultSet.getTime(1));
        Assertions.assertTrue(resultSet.wasNull());
        Assertions.assertNull(resultSet.getTimestamp(1));
        Assertions.assertTrue(resultSet.wasNull());
    }

    @Test
    void testBooleanType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN true as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean(1));
        Assertions.assertEquals(((Boolean) true).toString(), resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
    }

    @Test
    void testSimpleNumericType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN 1 as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(1L, resultSet.getLong(1));
        Assertions.assertEquals(1, resultSet.getInt(1));
        Assertions.assertEquals((short) 1, resultSet.getShort(1));
        Assertions.assertEquals((byte) 1, resultSet.getByte(1));
        Assertions.assertEquals(((Integer) 1).toString(), resultSet.getString(1));
        Assertions.assertTrue(resultSet.getBoolean(1));
        Assertions.assertEquals(new java.sql.Date(1L), resultSet.getDate(1));
        Assertions.assertEquals(new java.sql.Time(1L).toLocalTime(), resultSet.getTime(1).toLocalTime());
        Assertions.assertEquals(new java.sql.Timestamp(1L), resultSet.getTimestamp(1));
    }

    @Test
    void testLargeNumericType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN 4147483647 as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(4147483647L, resultSet.getLong(1));
        Assertions.assertEquals((int) 4147483647L, resultSet.getInt(1));
        Assertions.assertEquals((short) 4147483647L, resultSet.getShort(1));
        Assertions.assertEquals((byte) 4147483647L, resultSet.getByte(1));
        Assertions.assertEquals(((Long) 4147483647L).toString(), resultSet.getString(1));
        Assertions.assertTrue(resultSet.getBoolean(1));
        Assertions.assertEquals(new java.sql.Date(4147483647L), resultSet.getDate(1));
        Assertions.assertEquals(new java.sql.Time(4147483647L).toLocalTime(), resultSet.getTime(1).toLocalTime());
        Assertions.assertEquals(new java.sql.Timestamp(4147483647L), resultSet.getTimestamp(1));
    }

    @Test
    void testFloatingPointType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN 1.0 as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(1.0f, resultSet.getFloat(1));
        Assertions.assertEquals(1.0, resultSet.getDouble(1));
        Assertions.assertEquals((byte) 1.0, resultSet.getByte(1));
        Assertions.assertEquals((short) 1.0, resultSet.getShort(1));
        Assertions.assertEquals((int) 1.0, resultSet.getInt(1));
        Assertions.assertEquals((long) 1.0, resultSet.getLong(1));
        Assertions.assertEquals(((Double) 1.0).toString(), resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
    }

    @Test
    void testStringType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN 'hello' as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(resultSet.getString(1), "hello");
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
    }

    @Test
    void testNumericStringType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN '1.0' as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(resultSet.getString(1), "1.0");
        Assertions.assertEquals(resultSet.getString(1), ((Double) 1.0).toString());
        Assertions.assertEquals(resultSet.getFloat(1), 1.0f);
        Assertions.assertEquals(resultSet.getDouble(1), 1.0);
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
    }

    // Composite types
    @Test
    void testArrayType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN ['hello', 'world'] as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("[hello, world]", resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
    }

    @Test
    void testMapType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN ({hello:'world'}) as x");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("{hello=world}", resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
    }

    @Test
    void testNodeType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("CREATE (node:Foo {hello:'world'}) RETURN node");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(String.format("(%s : %s)", "[Foo]", "{hello=world}"), resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }

    @Test
    void testRelationshipType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery("CREATE (node1:Foo)-[rel:Rel {hello:'world'}]->(node2:Bar) RETURN rel");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(String.format("[%s : %s]", "Rel", "{hello=world}"), resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }


    @Test
    void testBiDirectionalPathType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(
                "CREATE p=(lyn:Person { name:'Lyndon'})-[:WORKS {position:'developer'}]->(bqt:Company {product:'software'})<-[:WORKS {position:'developer'}]-(val:Person { name:'Valentina'}) RETURN p");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(String.format("(%s)-[%s]->(%s)<-[%s]-(%s)",
                "[Person] : {name=Lyndon}", "WORKS : {position=developer}",
                "[Company] : {product=software}", "WORKS : {position=developer}", "[Person] : {name=Valentina}"),
                resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }


    @Test
    void testReverseDirectionalPathType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(
                "CREATE p=(node1:Foo)<-[rel:Rel]-(node2:Bar) RETURN p");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(String.format("(%s)<-[%s]-(%s)", "[Foo] : {}", "Rel : {}", "[Bar] : {}"),
                resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }


    @Test
    void testForwardDirectionalPathType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(
                "CREATE p=(node1:Foo)-[rel:Rel]->(node2:Bar) RETURN p");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(String.format("(%s)-[%s]->(%s)", "[Foo] : {}", "Rel : {}", "[Bar] : {}"),
                resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }

    @Test
    void test2DPointType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN point({ x:0, y:1 }) AS n");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(String.format("(%f, %f)", 0f, 1f), resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }

    @Test
    void test3DPointType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN point({ x:0, y:1, z:2 }) AS n");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(String.format("(%f, %f, %f)", 0f, 1f, 2f), resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }

    @Test
    void testDateType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN date(\"1993-03-30\") AS n");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("1993-03-30", resultSet.getString(1));
        Assertions.assertEquals(java.sql.Date.valueOf("1993-03-30"), resultSet.getDate(1));
        Assertions.assertEquals(java.sql.Timestamp.valueOf(LocalDateTime.of(1993, 3, 30, 0, 0)),
                resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }

    @Test
    void testTimeType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN time(\"12:10:10.000000225+0100\") AS n");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("12:10:10.000000225+01:00", resultSet.getString(1));
        Assertions.assertEquals(java.sql.Timestamp.valueOf(
                LocalTime.of(12, 10, 10, 225).atDate(LocalDate.ofEpochDay(0))),
                resultSet.getTimestamp(1));
        Assertions.assertEquals(java.sql.Time.valueOf("12:10:10"), resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }

    @Test
    void testLocalTimeType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN localtime(\"12:10:10.000000225\") AS n");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("12:10:10.000000225", resultSet.getString(1));
        Assertions.assertEquals(java.sql.Timestamp.valueOf(
                LocalTime.of(12, 10, 10, 225).atDate(LocalDate.ofEpochDay(0))),
                resultSet.getTimestamp(1));
        Assertions.assertEquals(java.sql.Time.valueOf("12:10:10"), resultSet.getTime(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }

    @Test
    void testDatetimeType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery("RETURN datetime(\"1993-03-30T12:10:10.000000225+0100\") AS n");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("1993-03-30T12:10:10.000000225+01:00", resultSet.getString(1));
        Assertions.assertEquals(java.sql.Date.valueOf("1993-03-30"), resultSet.getDate(1));
        Assertions.assertEquals(java.sql.Time.valueOf("12:10:10"), resultSet.getTime(1));
        Assertions.assertEquals(java.sql.Timestamp.valueOf(LocalDateTime.of(1993, 3, 30, 12, 10, 10, 225)),
                resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }

    @Test
    void testLocalDatetimeType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery("RETURN localdatetime(\"1993-03-30T12:10:10.000000225\") AS n");
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("1993-03-30T12:10:10.000000225", resultSet.getString(1));
        Assertions.assertEquals(java.sql.Date.valueOf("1993-03-30"), resultSet.getDate(1));
        Assertions.assertEquals(java.sql.Time.valueOf("12:10:10"), resultSet.getTime(1));
        Assertions.assertEquals(java.sql.Timestamp.valueOf(LocalDateTime.of(1993, 3, 30, 12, 10, 10, 225)),
                resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
    }

    @Test
    void testDurationType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("RETURN duration(\"P5M1.5D\") as n");
        Assertions.assertTrue(resultSet.next());

    }
}
