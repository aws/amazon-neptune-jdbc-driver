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

package software.aws.neptune.sparql.resultset;

import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import software.aws.neptune.sparql.SparqlConnection;
import software.aws.neptune.sparql.SparqlConnectionProperties;
import software.aws.neptune.sparql.mock.SparqlMockDataQuery;
import software.aws.neptune.sparql.mock.SparqlMockServer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class SparqlResultSetTest {
    private static final String HOSTNAME = "http://localhost";
    private static final String DATASET = "mock";
    private static final String QUERY_ENDPOINT = "query";
    private static final int PORT = SparqlMockServer.port(); // Mock server dynamically generates port
    private static final int SUBJECT_COLUMN_INDEX = 1;
    private static final int PREDICATE_COLUMN_INDEX = 2;
    private static final int OBJECT_COLUMN_INDEX = 3;
    private static final int SELECT_RESULT_INDEX = 2;
    private static final BigInteger EXPECTED_BIG_INTEGER_VALUE = new BigInteger("18446744073709551615");
    private static final BigDecimal EXPECTED_BIG_DECIMAL_VALUE = new BigDecimal("180.5555");
    private static final int EXPECTED_BYTE_VALUE = 127;
    private static final int EXPECTED_SHORT_VALUE = 32767;
    private static final Long EXPECTED_LONG_VALUE = 3000000000L;
    private static final double EXPECTED_DOUBLE_VALUE = 100000.00;
    private static final float EXPECTED_FLOAT_VALUE = 80.5f;
    private static final Long EXPECTED_UNSIGNED_LONG_VALUE = 4294970000L;
    private static final String EXPECTED_YEAR_VALUE = "2020";
    private static java.sql.Connection connection;
    private static java.sql.Statement statement;

    private static Properties sparqlProperties() {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        properties.put(SparqlConnectionProperties.ENDPOINT_KEY, HOSTNAME);
        properties.put(SparqlConnectionProperties.PORT_KEY, PORT);
        properties.put(SparqlConnectionProperties.DATASET_KEY, DATASET);
        properties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, QUERY_ENDPOINT);
        return properties;
    }

    /**
     * Function to start the mock server and populate database before testing.
     */
    @BeforeAll
    public static void initializeMockServer() throws SQLException {
        SparqlMockServer.ctlBeforeClass();

        // Query only.
        final RDFConnectionRemoteBuilder rdfConnBuilder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                .queryEndpoint("/query");

        // Load dataset in.
        try (final RDFConnection conn = rdfConnBuilder.build()) {
            conn.load("src/test/java/software/aws/neptune/sparql/mock/sparql_mock_data.rdf");
        }
    }

    /**
     * Function to tear down server after testing.
     */
    @AfterAll
    public static void shutdownMockServer() {
        SparqlMockServer.ctlAfterClass();
    }

    // helper function for testing queries with Java String outputs
    private static void testStringResultTypes(final String query, final String expectedValue, final int columnIdx)
            throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(query);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(expectedValue, resultSet.getString(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getString(0));
        expectNumericTypesToThrow(resultSet, columnIdx);
    }

    // helper function for testing queries with Java Integer outputs
    private static void testIntegerResultTypes(final String query, final int expectedValue, final int columnIdx)
            throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(query);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(getExpectedByteValue(expectedValue), resultSet.getByte(columnIdx));
        Assertions.assertEquals(getExpectedShortValue(expectedValue), resultSet.getShort(columnIdx));
        Assertions.assertEquals(expectedValue, resultSet.getInt(columnIdx));
        Assertions.assertEquals(expectedValue, resultSet.getLong(columnIdx));
        Assertions.assertEquals(String.valueOf(expectedValue), resultSet.getString(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getString(0));
    }

    private static void expectNumericTypesToThrow(final ResultSet resultSet, final int columnIdx) {
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBigDecimal(columnIdx));
    }

    @BeforeEach
    void initialize() throws SQLException {
        connection = new SparqlConnection(new SparqlConnectionProperties(sparqlProperties()));
        statement = connection.createStatement();
    }

    @AfterEach
    void shutdown() throws SQLException {
        connection.close();
    }

    @Test
    void testNullType() throws SQLException {
        final java.sql.ResultSet resultSet = statement
                .executeQuery("SELECT ?s ?x ?fname WHERE {?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  ?fname}");
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
    void testStringType() throws SQLException {
        testStringResultTypes(SparqlMockDataQuery.STRING_QUERY, "http://somewhere/JohnSmith", 1);
        testStringResultTypes(SparqlMockDataQuery.STRING_QUERY, "John Smith", SELECT_RESULT_INDEX);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY,
                "http://somewhere/JohnSmith", SUBJECT_COLUMN_INDEX);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY,
                "http://www.w3.org/2001/vcard-rdf/3.0#FN", PREDICATE_COLUMN_INDEX);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY,
                "John Smith", OBJECT_COLUMN_INDEX);
    }

    private static byte getExpectedByteValue(final long value) {
        return (value > Byte.MAX_VALUE) ? 0 : (byte) value;
    }

    private static short getExpectedShortValue(final long value) {
        return (value > Short.MAX_VALUE) ? 0 : (short) value;
    }

    private static int getExpectedIntValue(final long value) {
        return (value > Integer.MAX_VALUE) ? 0 : (int) value;
    }

    @Test
    void testBooleanType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.BOOL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean(SELECT_RESULT_INDEX));
        Assertions.assertEquals(String.valueOf(true), resultSet.getString(SELECT_RESULT_INDEX));
        expectNumericTypesToThrow(resultSet, SELECT_RESULT_INDEX);
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(SELECT_RESULT_INDEX));
    }

    @Test
    void testConstructBooleanType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_BOOL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(String.valueOf(true), resultSet.getString(OBJECT_COLUMN_INDEX));
        expectNumericTypesToThrow(resultSet, OBJECT_COLUMN_INDEX);
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testByteType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.BYTE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(getExpectedByteValue(EXPECTED_BYTE_VALUE), resultSet.getByte(SELECT_RESULT_INDEX));
        Assertions.assertEquals((short) EXPECTED_BYTE_VALUE, resultSet.getShort(SELECT_RESULT_INDEX));
        Assertions.assertEquals(EXPECTED_BYTE_VALUE, resultSet.getInt(SELECT_RESULT_INDEX));
        Assertions.assertEquals(EXPECTED_BYTE_VALUE, resultSet.getLong(SELECT_RESULT_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_BYTE_VALUE), resultSet.getString(SELECT_RESULT_INDEX));
    }

    @Test
    void testConstructByteType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_BYTE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(getExpectedByteValue(EXPECTED_BYTE_VALUE), resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedShortValue(EXPECTED_BYTE_VALUE), resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(EXPECTED_BYTE_VALUE, resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(EXPECTED_BYTE_VALUE, resultSet.getLong(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_BYTE_VALUE), resultSet.getString(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testShortType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.SHORT_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(getExpectedByteValue(EXPECTED_SHORT_VALUE), resultSet.getByte(SELECT_RESULT_INDEX));
        Assertions.assertEquals((short) EXPECTED_SHORT_VALUE, resultSet.getShort(SELECT_RESULT_INDEX));
        Assertions.assertEquals(EXPECTED_SHORT_VALUE, resultSet.getInt(SELECT_RESULT_INDEX));
        Assertions.assertEquals(EXPECTED_SHORT_VALUE, resultSet.getLong(SELECT_RESULT_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_SHORT_VALUE), resultSet.getString(SELECT_RESULT_INDEX));
    }

    @Test
    void testConstructShortType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_SHORT_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(getExpectedByteValue(EXPECTED_SHORT_VALUE), resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedShortValue(EXPECTED_SHORT_VALUE), resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(EXPECTED_SHORT_VALUE, resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(EXPECTED_SHORT_VALUE, resultSet.getLong(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_SHORT_VALUE), resultSet.getString(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testIntegerSmallType() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.INTEGER_SMALL_QUERY, 25, SELECT_RESULT_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_INTEGER_SMALL_QUERY, 25, OBJECT_COLUMN_INDEX);
    }

    @Test
    void testIntegerLargeType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.INTEGER_LARGE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_BIG_INTEGER_VALUE, resultSet.getObject(SELECT_RESULT_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_BIG_INTEGER_VALUE), resultSet.getString(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testConstructIntegerLargeType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_INTEGER_LARGE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_BIG_INTEGER_VALUE, resultSet.getObject(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_BIG_INTEGER_VALUE), resultSet.getString(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testLongType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.LONG_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(getExpectedByteValue(EXPECTED_LONG_VALUE), resultSet.getByte(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedShortValue(EXPECTED_LONG_VALUE), resultSet.getShort(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedIntValue(EXPECTED_LONG_VALUE), resultSet.getInt(SELECT_RESULT_INDEX));
        Assertions.assertEquals(EXPECTED_LONG_VALUE, resultSet.getLong(SELECT_RESULT_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_LONG_VALUE), resultSet.getString(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(SELECT_RESULT_INDEX));
    }

    @Test
    void testConstructLongType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_LONG_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(getExpectedByteValue(EXPECTED_LONG_VALUE), resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedShortValue(EXPECTED_LONG_VALUE), resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedIntValue(EXPECTED_LONG_VALUE), resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(EXPECTED_LONG_VALUE, resultSet.getLong(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_LONG_VALUE), resultSet.getString(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(OBJECT_COLUMN_INDEX).toLocalTime());
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testIntType() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.INT_QUERY, -100, SELECT_RESULT_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_INT_QUERY, -100, OBJECT_COLUMN_INDEX);
    }

    @Test
    void testBigDecimalType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.DECIMAL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_BIG_DECIMAL_VALUE, resultSet.getBigDecimal(SELECT_RESULT_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_BIG_DECIMAL_VALUE), resultSet.getString(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedByteValue(EXPECTED_BIG_DECIMAL_VALUE.longValue()), resultSet.getByte(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedShortValue(EXPECTED_BIG_DECIMAL_VALUE.shortValue()), resultSet.getShort(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedIntValue(EXPECTED_BIG_DECIMAL_VALUE.longValue()), resultSet.getInt(SELECT_RESULT_INDEX));
        Assertions.assertEquals(EXPECTED_BIG_DECIMAL_VALUE.floatValue(), resultSet.getFloat(SELECT_RESULT_INDEX));
        Assertions.assertEquals(EXPECTED_BIG_DECIMAL_VALUE.doubleValue(), resultSet.getDouble(SELECT_RESULT_INDEX));
        Assertions.assertEquals(EXPECTED_BIG_DECIMAL_VALUE.longValue(), resultSet.getLong(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(SELECT_RESULT_INDEX));
    }

    @Test
    void testConstructBigDecimalType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_DECIMAL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_BIG_DECIMAL_VALUE, resultSet.getBigDecimal(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_BIG_DECIMAL_VALUE), resultSet.getString(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedByteValue(EXPECTED_BIG_DECIMAL_VALUE.longValue()), resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedShortValue(EXPECTED_BIG_DECIMAL_VALUE.longValue()), resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedIntValue(EXPECTED_BIG_DECIMAL_VALUE.longValue()), resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(EXPECTED_BIG_DECIMAL_VALUE.floatValue(), resultSet.getFloat(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(EXPECTED_BIG_DECIMAL_VALUE.doubleValue(), resultSet.getDouble(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(EXPECTED_BIG_DECIMAL_VALUE.longValue(), resultSet.getLong(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testDoubleType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.DOUBLE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_DOUBLE_VALUE, resultSet.getFloat(SELECT_RESULT_INDEX));
        Assertions.assertEquals(EXPECTED_DOUBLE_VALUE, resultSet.getDouble(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedByteValue((long) EXPECTED_DOUBLE_VALUE), resultSet.getByte(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedShortValue((long) EXPECTED_DOUBLE_VALUE), resultSet.getShort(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedIntValue((long) EXPECTED_DOUBLE_VALUE), resultSet.getInt(SELECT_RESULT_INDEX));
        Assertions.assertEquals((long) EXPECTED_DOUBLE_VALUE, resultSet.getLong(SELECT_RESULT_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_DOUBLE_VALUE), resultSet.getString(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(SELECT_RESULT_INDEX));
    }

    @Test
    void testConstructDoubleType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_DOUBLE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_DOUBLE_VALUE, resultSet.getFloat(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(EXPECTED_DOUBLE_VALUE, resultSet.getDouble(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedByteValue((long) EXPECTED_DOUBLE_VALUE), resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedShortValue((long) EXPECTED_DOUBLE_VALUE), resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedIntValue((long) EXPECTED_DOUBLE_VALUE), resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals((long) EXPECTED_DOUBLE_VALUE, resultSet.getLong(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_DOUBLE_VALUE), resultSet.getString(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testFloatType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.FLOAT_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_FLOAT_VALUE, resultSet.getFloat(SELECT_RESULT_INDEX));
        Assertions.assertEquals(EXPECTED_FLOAT_VALUE, resultSet.getDouble(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedByteValue((long) EXPECTED_FLOAT_VALUE), resultSet.getByte(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedShortValue((long) EXPECTED_FLOAT_VALUE), resultSet.getShort(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedIntValue((long) EXPECTED_FLOAT_VALUE), resultSet.getInt(SELECT_RESULT_INDEX));
        Assertions.assertEquals((long) EXPECTED_FLOAT_VALUE, resultSet.getLong(SELECT_RESULT_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_FLOAT_VALUE), resultSet.getString(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(SELECT_RESULT_INDEX));
    }

    @Test
    void testConstructFloatType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_FLOAT_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_FLOAT_VALUE, resultSet.getFloat(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(EXPECTED_FLOAT_VALUE, resultSet.getDouble(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedByteValue((long) EXPECTED_FLOAT_VALUE), resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedShortValue((long) EXPECTED_FLOAT_VALUE), resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedIntValue((long) EXPECTED_FLOAT_VALUE), resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals((long) EXPECTED_FLOAT_VALUE, resultSet.getLong(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_FLOAT_VALUE), resultSet.getString(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testUnsignedByteType() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.UNSIGNED_BYTE_QUERY, 200, SELECT_RESULT_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_UNSIGNED_BYTE_QUERY, 200, OBJECT_COLUMN_INDEX);
    }

    @Test
    void testUnsignedShortType() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.UNSIGNED_SHORT_QUERY, 300, SELECT_RESULT_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_UNSIGNED_SHORT_QUERY, 300, OBJECT_COLUMN_INDEX);
    }

    @Test
    void testUnsignedIntType() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.UNSIGNED_INT_QUERY, 65600, SELECT_RESULT_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_UNSIGNED_INT_QUERY, 65600, OBJECT_COLUMN_INDEX);
    }

    @Test
    void testUnsignedLongSmallType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.UNSIGNED_LONG_SMALL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(getExpectedByteValue(EXPECTED_UNSIGNED_LONG_VALUE), resultSet.getByte(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedShortValue(EXPECTED_UNSIGNED_LONG_VALUE), resultSet.getInt(SELECT_RESULT_INDEX));
        Assertions.assertEquals(getExpectedIntValue(EXPECTED_UNSIGNED_LONG_VALUE), resultSet.getInt(SELECT_RESULT_INDEX));
        Assertions.assertEquals(EXPECTED_UNSIGNED_LONG_VALUE, resultSet.getLong(SELECT_RESULT_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_UNSIGNED_LONG_VALUE), resultSet.getString(SELECT_RESULT_INDEX));
    }

    @Test
    void testConstructUnsignedLongSmallType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_UNSIGNED_LONG_SMALL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(getExpectedByteValue(EXPECTED_UNSIGNED_LONG_VALUE), resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedShortValue(EXPECTED_UNSIGNED_LONG_VALUE), resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(getExpectedIntValue(EXPECTED_UNSIGNED_LONG_VALUE), resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(EXPECTED_UNSIGNED_LONG_VALUE, resultSet.getLong(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_UNSIGNED_LONG_VALUE), resultSet.getString(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testUnsignedLongLargeType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.UNSIGNED_LONG_LARGE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_BIG_INTEGER_VALUE, resultSet.getObject(SELECT_RESULT_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_BIG_INTEGER_VALUE), resultSet.getString(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBigDecimal(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(SELECT_RESULT_INDEX));
    }

    @Test
    void testConstructUnsignedLongLargeType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_UNSIGNED_LONG_LARGE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_BIG_INTEGER_VALUE, resultSet.getObject(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(String.valueOf(EXPECTED_BIG_INTEGER_VALUE), resultSet.getString(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testSmallRangedIntegerTypes() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.POSITIVE_INTEGER_QUERY, 5, SELECT_RESULT_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.NON_NEGATIVE_INTEGER_QUERY, 1, SELECT_RESULT_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.NEGATIVE_INTEGER_QUERY, -5, SELECT_RESULT_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.NON_POSITIVE_INTEGER_QUERY, -1, SELECT_RESULT_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_POSITIVE_INTEGER_QUERY, 5, OBJECT_COLUMN_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_NON_NEGATIVE_INTEGER_QUERY, 1, OBJECT_COLUMN_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_NEGATIVE_INTEGER_QUERY, -5, OBJECT_COLUMN_INDEX);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_NON_POSITIVE_INTEGER_QUERY, -1, OBJECT_COLUMN_INDEX);
    }

    @Test
    void testDateType() throws SQLException {
        final ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.DATE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("1996-01-01", resultSet.getString(SELECT_RESULT_INDEX));
        Assertions.assertEquals(java.sql.Date.valueOf("1996-01-01"), resultSet.getDate(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(SELECT_RESULT_INDEX));
        expectNumericTypesToThrow(resultSet, SELECT_RESULT_INDEX);
    }

    @Test
    void testConstructDateType() throws SQLException {
        final ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_DATE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("1996-01-01", resultSet.getString(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(java.sql.Date.valueOf("1996-01-01"), resultSet.getDate(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(OBJECT_COLUMN_INDEX));
        expectNumericTypesToThrow(resultSet, OBJECT_COLUMN_INDEX);
    }

    @Test
    void testTimeType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.TIME_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("22:10:10", resultSet.getString(SELECT_RESULT_INDEX));
        Assertions.assertEquals(java.sql.Time.valueOf("22:10:10"), resultSet.getTime(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(SELECT_RESULT_INDEX));
        expectNumericTypesToThrow(resultSet, SELECT_RESULT_INDEX);
    }

    @Test
    void testConstructTimeType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_TIME_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("22:10:10", resultSet.getString(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(java.sql.Time.valueOf("22:10:10"), resultSet.getTime(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(OBJECT_COLUMN_INDEX));
        expectNumericTypesToThrow(resultSet, OBJECT_COLUMN_INDEX);
    }

    @Test
    void testDateTimeType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.DATE_TIME_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020-01-01 00:10:10.0", resultSet.getString(SELECT_RESULT_INDEX));
        Assertions.assertEquals(java.sql.Time.valueOf("00:10:10").toString(),
                resultSet.getTime(SELECT_RESULT_INDEX).toString());
        Assertions.assertEquals(java.sql.Date.valueOf("2020-01-01").toString(),
                resultSet.getDate(SELECT_RESULT_INDEX).toString());
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(SELECT_RESULT_INDEX));
        expectNumericTypesToThrow(resultSet, SELECT_RESULT_INDEX);
    }

    @Test
    void testConstructDateTimeType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_DATE_TIME_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020-01-01 00:10:10.0", resultSet.getString(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(java.sql.Time.valueOf("00:10:10").toString(),
                resultSet.getTime(OBJECT_COLUMN_INDEX).toString());
        Assertions.assertEquals(java.sql.Date.valueOf("2020-01-01").toString(),
                resultSet.getDate(OBJECT_COLUMN_INDEX).toString());
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(OBJECT_COLUMN_INDEX));
        expectNumericTypesToThrow(resultSet, OBJECT_COLUMN_INDEX);
    }

    @Test
    void testDateTimeStampType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.DATE_TIME_STAMP_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020-01-01T06:10:10Z[UTC]", resultSet.getString(SELECT_RESULT_INDEX));
        Assertions.assertEquals(java.sql.Time.valueOf("06:10:10").toString(),
                resultSet.getTime(SELECT_RESULT_INDEX).toString());
        Assertions.assertEquals(java.sql.Date.valueOf("2020-01-01").toString(),
                resultSet.getDate(SELECT_RESULT_INDEX).toString());
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(SELECT_RESULT_INDEX));
        expectNumericTypesToThrow(resultSet, SELECT_RESULT_INDEX);
    }

    @Test
    void testConstructDateTimeStampType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_DATE_TIME_STAMP_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020-01-01T06:10:10Z[UTC]", resultSet.getString(OBJECT_COLUMN_INDEX));
        Assertions.assertEquals(java.sql.Time.valueOf("06:10:10").toString(),
                resultSet.getTime(OBJECT_COLUMN_INDEX).toString());
        Assertions.assertEquals(java.sql.Date.valueOf("2020-01-01").toString(),
                resultSet.getDate(OBJECT_COLUMN_INDEX).toString());
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(OBJECT_COLUMN_INDEX));
        expectNumericTypesToThrow(resultSet, OBJECT_COLUMN_INDEX);
    }

    @Test
    void testGYearType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.G_YEAR_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_YEAR_VALUE, resultSet.getString(SELECT_RESULT_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBigDecimal(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testConstructGYearType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_G_YEAR_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(EXPECTED_YEAR_VALUE, resultSet.getString(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBigDecimal(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(OBJECT_COLUMN_INDEX));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(OBJECT_COLUMN_INDEX));
    }

    @Test
    void testGMonthType() throws SQLException {
        testStringResultTypes(SparqlMockDataQuery.G_MONTH_QUERY, "--10", SELECT_RESULT_INDEX);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_G_MONTH_QUERY, "--10", OBJECT_COLUMN_INDEX);
    }

    @Test
    void testGDayType() throws SQLException {
        testStringResultTypes(SparqlMockDataQuery.G_DAY_QUERY, "---20", SELECT_RESULT_INDEX);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_G_DAY_QUERY, "---20", OBJECT_COLUMN_INDEX);
    }

    @Test
    void testGYearMonthType() throws SQLException {
        testStringResultTypes(SparqlMockDataQuery.G_YEAR_MONTH_QUERY, "2020-06", SELECT_RESULT_INDEX);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_G_YEAR_MONTH_QUERY, "2020-06", OBJECT_COLUMN_INDEX);
    }

    @Test
    void testGMonthDayType() throws SQLException {
        testStringResultTypes(SparqlMockDataQuery.G_MONTH_DAY_QUERY, "--06-01", SELECT_RESULT_INDEX);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_G_MONTH_DAY_QUERY, "--06-01", OBJECT_COLUMN_INDEX);
    }

    @Test
    void testDurationTypes() throws SQLException {
        testStringResultTypes(SparqlMockDataQuery.DURATION_QUERY, "P30D", SELECT_RESULT_INDEX);
        testStringResultTypes(SparqlMockDataQuery.YEAR_MONTH_DURATION_QUERY, "P2M", SELECT_RESULT_INDEX);
        testStringResultTypes(SparqlMockDataQuery.DAY_TIME_DURATION_QUERY, "P5D", SELECT_RESULT_INDEX);

        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_DURATION_QUERY, "P30D", OBJECT_COLUMN_INDEX);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_YEAR_MONTH_DURATION_QUERY, "P2M", OBJECT_COLUMN_INDEX);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_DAY_TIME_DURATION_QUERY, "P5D", OBJECT_COLUMN_INDEX);
    }

    @Test
    void testEmptySelectResult() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.EMPTY_SELECT_RESULT_QUERY);
        Assertions.assertThrows(SQLException.class, () -> resultSet.getString(SELECT_RESULT_INDEX));
    }

    @Test
    void testAskQueryType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.ASK_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean(1));
        Assertions.assertEquals(String.valueOf(true), resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getString(SELECT_RESULT_INDEX));
        expectNumericTypesToThrow(resultSet, 1);
        Assertions.assertFalse(resultSet.next());
    }

    // DESCRIBE and CONSTRUCT share ResultSet formats, so we just use this to test that this query type works.
    @Test
    void testDescribeQueryType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("DESCRIBE <http://somewhere/JohnSmith>");
        while (resultSet.next()) {
            Assertions.assertNotNull(resultSet.getString(SUBJECT_COLUMN_INDEX));
            Assertions.assertNotNull(resultSet.getString(PREDICATE_COLUMN_INDEX));
            Assertions.assertNotNull(resultSet.getString(OBJECT_COLUMN_INDEX));
        }
    }
}
