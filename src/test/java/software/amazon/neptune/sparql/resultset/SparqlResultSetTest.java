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

package software.amazon.neptune.sparql.resultset;

import org.apache.jena.atlas.iterator.PeekIterator;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.jdbc.utilities.JdbcType;
import software.amazon.neptune.sparql.SparqlConnection;
import software.amazon.neptune.sparql.SparqlConnectionProperties;
import software.amazon.neptune.sparql.mock.SparqlMockDataQuery;
import software.amazon.neptune.sparql.mock.SparqlMockServer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SparqlResultSetTest {
    private static final String HOSTNAME = "http://localhost";
    private static final String DATASET = "mock";
    private static final String QUERY_ENDPOINT = "query";
    private static final int PORT = SparqlMockServer.port(); // Mock server dynamically generates port
    private static java.sql.Connection connection;
    private static RDFConnectionRemoteBuilder rdfConnBuilder;
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
    public static void ctlBeforeClass() throws SQLException {
        SparqlMockServer.ctlBeforeClass();

        // TODO: refactor this data insertion else where (e.g. mock server)?
        // insert into the database here
        rdfConnBuilder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                // Query only.
                .queryEndpoint("/query")
                .updateEndpoint("/update");

        // load dataset in
        try (final RDFConnection conn = rdfConnBuilder.build()) {
            conn.load("src/test/java/software/amazon/neptune/sparql/mock/sparql_mock_data.rdf");
        }
    }

    /**
     * Function to tear down server after testing.
     */
    @AfterAll
    public static void ctlAfterClass() {
        SparqlMockServer.ctlAfterClass();
    }

    // helper function for testing queries with Java String outputs
    private static void testStringResultTypes(final String query, final String expectedValue, final int columnIdx)
            throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(query);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(expectedValue, resultSet.getString(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBigDecimal(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTime(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDate(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getString(0));
    }

    // helper function for testing queries with Java Integer outputs
    private static void testIntegerResultTypes(final String query, final int expectedValue, final int columnIdx)
            throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(query);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals((byte) expectedValue, resultSet.getByte(columnIdx));
        Assertions.assertEquals((short) expectedValue, resultSet.getShort(columnIdx));
        Assertions.assertEquals(expectedValue, resultSet.getInt(columnIdx));
        Assertions.assertEquals(expectedValue, resultSet.getLong(columnIdx));
        Assertions.assertEquals(String.valueOf(expectedValue), resultSet.getString(columnIdx));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getString(0));
    }

    // to printout result in format of Jena ResultSet
    private static void printJenaResultSetOut(final String query) {
        final Query jenaQuery = QueryFactory.create(query);
        try (final RDFConnection conn = rdfConnBuilder.build()) {
            conn.queryResultSet(jenaQuery, ResultSetFormatter::out);
        }
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
        testStringResultTypes(SparqlMockDataQuery.STRING_QUERY, "John Smith", 2);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY, "http://www.w3.org/2001/vcard-rdf/3.0#FN", 1);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY, "http://somewhere/JohnSmith", 2);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY, "John Smith", 3);
    }

    @Test
    void testBooleanType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.BOOL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean(2));
        Assertions.assertEquals(String.valueOf(true), resultSet.getString(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBigDecimal(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(2));
    }

    @Test
    void testConstructBooleanType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_BOOL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean(3));
        Assertions.assertEquals(String.valueOf(true), resultSet.getString(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBigDecimal(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(3));
    }

    @Test
    void testByteType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.BYTE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals((byte) 127, resultSet.getByte(2));
        Assertions.assertEquals((short) 127, resultSet.getShort(2));
        Assertions.assertEquals(127, resultSet.getInt(2));
        Assertions.assertEquals(127L, resultSet.getLong(2));
        Assertions.assertEquals(String.valueOf(127), resultSet.getString(2));
    }

    @Test
    void testConstructByteType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_BYTE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals((byte) 127, resultSet.getByte(3));
        Assertions.assertEquals((short) 127, resultSet.getShort(3));
        Assertions.assertEquals(127, resultSet.getInt(3));
        Assertions.assertEquals(127L, resultSet.getLong(3));
        Assertions.assertEquals(String.valueOf(127), resultSet.getString(3));
    }

    @Test
    void testShortType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.SHORT_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals((byte) 32767, resultSet.getByte(2));
        Assertions.assertEquals((short) 32767, resultSet.getShort(2));
        Assertions.assertEquals(32767, resultSet.getInt(2));
        Assertions.assertEquals(32767L, resultSet.getLong(2));
        Assertions.assertEquals(String.valueOf(32767), resultSet.getString(2));
    }

    @Test
    void testConstructShortType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_SHORT_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals((byte) 32767, resultSet.getByte(3));
        Assertions.assertEquals((short) 32767, resultSet.getShort(3));
        Assertions.assertEquals(32767, resultSet.getInt(3));
        Assertions.assertEquals(32767L, resultSet.getLong(3));
        Assertions.assertEquals(String.valueOf(32767), resultSet.getString(3));
    }

    @Test
    void testIntegerSmallType() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.INTEGER_SMALL_QUERY, 25, 2);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_INTEGER_SMALL_QUERY, 25, 3);
    }

    @Test
    void testIntegerLargeType() throws SQLException {
        final BigInteger expectedValue = new BigInteger("18446744073709551615");
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.INTEGER_LARGE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(expectedValue.byteValue(), resultSet.getByte(2));
        Assertions.assertEquals(expectedValue.shortValue(), resultSet.getShort(2));
        Assertions.assertEquals(expectedValue.intValue(), resultSet.getInt(2));
        Assertions.assertEquals(expectedValue.doubleValue(), resultSet.getDouble(2));
        Assertions.assertEquals(expectedValue.floatValue(), resultSet.getFloat(2));
        Assertions.assertEquals(expectedValue.longValue(), resultSet.getLong(2));
        Assertions.assertEquals(expectedValue, resultSet.getObject(2));
        Assertions.assertEquals(String.valueOf(expectedValue), resultSet.getString(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(2));
    }

    @Test
    void testConstructIntegerLargeType() throws SQLException {
        final BigInteger expectedValue = new BigInteger("18446744073709551615");
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_INTEGER_LARGE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(expectedValue.byteValue(), resultSet.getByte(3));
        Assertions.assertEquals(expectedValue.shortValue(), resultSet.getShort(3));
        Assertions.assertEquals(expectedValue.intValue(), resultSet.getInt(3));
        Assertions.assertEquals(expectedValue.doubleValue(), resultSet.getDouble(3));
        Assertions.assertEquals(expectedValue.floatValue(), resultSet.getFloat(3));
        Assertions.assertEquals(expectedValue.longValue(), resultSet.getLong(3));
        Assertions.assertEquals(expectedValue, resultSet.getObject(3));
        Assertions.assertEquals(String.valueOf(expectedValue), resultSet.getString(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(3));
    }

    @Test
    void testLongType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.LONG_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals((byte) 3000000000L, resultSet.getByte(2));
        Assertions.assertEquals((short) 3000000000L, resultSet.getShort(2));
        Assertions.assertEquals((int) 3000000000L, resultSet.getInt(2));
        Assertions.assertEquals(3000000000L, resultSet.getLong(2));
        Assertions.assertEquals(String.valueOf(3000000000L), resultSet.getString(2));
        Assertions.assertEquals(new java.sql.Date(3000000000L), resultSet.getDate(2));
        Assertions.assertEquals(new java.sql.Time(3000000000L).toLocalTime(), resultSet.getTime(2).toLocalTime());
        Assertions.assertEquals(new java.sql.Timestamp(3000000000L), resultSet.getTimestamp(2));
    }

    @Test
    void testConstructLongType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_LONG_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals((byte) 3000000000L, resultSet.getByte(3));
        Assertions.assertEquals((short) 3000000000L, resultSet.getShort(3));
        Assertions.assertEquals((int) 3000000000L, resultSet.getInt(3));
        Assertions.assertEquals(3000000000L, resultSet.getLong(3));
        Assertions.assertEquals(String.valueOf(3000000000L), resultSet.getString(3));
        Assertions.assertEquals(new java.sql.Date(3000000000L), resultSet.getDate(3));
        Assertions.assertEquals(new java.sql.Time(3000000000L).toLocalTime(), resultSet.getTime(3).toLocalTime());
        Assertions.assertEquals(new java.sql.Timestamp(3000000000L), resultSet.getTimestamp(3));
    }

    @Test
    void testIntType() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.INT_QUERY, -100, 2);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_INT_QUERY, -100, 3);
    }

    @Test
    void testBigDecimalType() throws SQLException {
        final BigDecimal expectedValue = new BigDecimal("180.5");
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.DECIMAL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(expectedValue, resultSet.getBigDecimal(2));
        Assertions.assertEquals(String.valueOf(expectedValue), resultSet.getString(2));
        Assertions.assertEquals(expectedValue.byteValue(), resultSet.getByte(2));
        Assertions.assertEquals(expectedValue.shortValue(), resultSet.getShort(2));
        Assertions.assertEquals(expectedValue.intValue(), resultSet.getInt(2));
        Assertions.assertEquals(expectedValue.floatValue(), resultSet.getFloat(2));
        Assertions.assertEquals(expectedValue.doubleValue(), resultSet.getDouble(2));
        Assertions.assertEquals(expectedValue.longValue(), resultSet.getLong(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(2));
    }

    @Test
    void testConstructBigDecimalType() throws SQLException {
        final BigDecimal expectedValue = new BigDecimal("180.5");
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_DECIMAL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(expectedValue, resultSet.getBigDecimal(3));
        Assertions.assertEquals(String.valueOf(expectedValue), resultSet.getString(3));
        Assertions.assertEquals(expectedValue.byteValue(), resultSet.getByte(3));
        Assertions.assertEquals(expectedValue.shortValue(), resultSet.getShort(3));
        Assertions.assertEquals(expectedValue.intValue(), resultSet.getInt(3));
        Assertions.assertEquals(expectedValue.floatValue(), resultSet.getFloat(3));
        Assertions.assertEquals(expectedValue.doubleValue(), resultSet.getDouble(3));
        Assertions.assertEquals(expectedValue.longValue(), resultSet.getLong(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(3));
    }

    @Test
    void testDoubleType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.DOUBLE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(100000.00f, resultSet.getFloat(2));
        Assertions.assertEquals(100000.00, resultSet.getDouble(2));
        Assertions.assertEquals((byte) 100000.00, resultSet.getByte(2));
        Assertions.assertEquals((short) 100000.00, resultSet.getShort(2));
        Assertions.assertEquals((int) 100000.00, resultSet.getInt(2));
        Assertions.assertEquals((long) 100000.00, resultSet.getLong(2));
        Assertions.assertEquals(String.valueOf(100000.00), resultSet.getString(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(2));
    }

    @Test
    void testConstructDoubleType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_DOUBLE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(100000.00f, resultSet.getFloat(3));
        Assertions.assertEquals(100000.00, resultSet.getDouble(3));
        Assertions.assertEquals((byte) 100000.00, resultSet.getByte(3));
        Assertions.assertEquals((short) 100000.00, resultSet.getShort(3));
        Assertions.assertEquals((int) 100000.00, resultSet.getInt(3));
        Assertions.assertEquals((long) 100000.00, resultSet.getLong(3));
        Assertions.assertEquals(String.valueOf(100000.00), resultSet.getString(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(3));
    }

    @Test
    void testFloatType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.FLOAT_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(80.5f, resultSet.getFloat(2));
        Assertions.assertEquals(80.5, resultSet.getDouble(2));
        Assertions.assertEquals((byte) 80.5, resultSet.getByte(2));
        Assertions.assertEquals((short) 80.5, resultSet.getShort(2));
        Assertions.assertEquals((int) 80.5, resultSet.getInt(2));
        Assertions.assertEquals((long) 80.5, resultSet.getLong(2));
        Assertions.assertEquals(String.valueOf(80.5f), resultSet.getString(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(2));
    }

    @Test
    void testConstructFloatType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_FLOAT_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(80.5f, resultSet.getFloat(3));
        Assertions.assertEquals(80.5, resultSet.getDouble(3));
        Assertions.assertEquals((byte) 80.5, resultSet.getByte(3));
        Assertions.assertEquals((short) 80.5, resultSet.getShort(3));
        Assertions.assertEquals((int) 80.5, resultSet.getInt(3));
        Assertions.assertEquals((long) 80.5, resultSet.getLong(3));
        Assertions.assertEquals(String.valueOf(80.5f), resultSet.getString(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(3));
    }

    @Test
    void testUnsignedByteType() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.UNSIGNED_BYTE_QUERY, 200, 2);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_UNSIGNED_BYTE_QUERY, 200, 3);
    }

    @Test
    void testUnsignedShortType() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.UNSIGNED_SHORT_QUERY, 300, 2);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_UNSIGNED_SHORT_QUERY, 300, 3);
    }

    @Test
    void testUnsignedIntType() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.UNSIGNED_INT_QUERY, 65600, 2);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_UNSIGNED_INT_QUERY, 65600, 3);
    }

    @Test
    void testUnsignedLongSmallType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.UNSIGNED_LONG_SMALL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals((byte) 4294970000L, resultSet.getByte(2));
        Assertions.assertEquals((short) 4294970000L, resultSet.getShort(2));
        Assertions.assertEquals((int) 4294970000L, resultSet.getInt(2));
        Assertions.assertEquals(4294970000L, resultSet.getLong(2));
        Assertions.assertEquals(String.valueOf(4294970000L), resultSet.getString(2));
    }

    @Test
    void testConstructUnsignedLongSmallType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_UNSIGNED_LONG_SMALL_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals((byte) 4294970000L, resultSet.getByte(3));
        Assertions.assertEquals((short) 4294970000L, resultSet.getShort(3));
        Assertions.assertEquals((int) 4294970000L, resultSet.getInt(3));
        Assertions.assertEquals(4294970000L, resultSet.getLong(3));
        Assertions.assertEquals(String.valueOf(4294970000L), resultSet.getString(3));
    }

    @Test
    void testUnsignedLongLargeType() throws SQLException {
        final BigInteger expectedValue = new BigInteger("18446744073709551615");
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.UNSIGNED_LONG_LARGE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(expectedValue.byteValue(), resultSet.getByte(2));
        Assertions.assertEquals(expectedValue.shortValue(), resultSet.getShort(2));
        Assertions.assertEquals(expectedValue.intValue(), resultSet.getInt(2));
        Assertions.assertEquals(expectedValue.doubleValue(), resultSet.getDouble(2));
        Assertions.assertEquals(expectedValue.floatValue(), resultSet.getFloat(2));
        Assertions.assertEquals(expectedValue.longValue(), resultSet.getLong(2));
        Assertions.assertEquals(expectedValue, resultSet.getObject(2));
        Assertions.assertEquals(String.valueOf(expectedValue), resultSet.getString(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(2));
    }

    @Test
    void testConstructUnsignedLongLargeType() throws SQLException {
        final BigInteger expectedValue = new BigInteger("18446744073709551615");
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_UNSIGNED_LONG_LARGE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(expectedValue.byteValue(), resultSet.getByte(3));
        Assertions.assertEquals(expectedValue.shortValue(), resultSet.getShort(3));
        Assertions.assertEquals(expectedValue.intValue(), resultSet.getInt(3));
        Assertions.assertEquals(expectedValue.doubleValue(), resultSet.getDouble(3));
        Assertions.assertEquals(expectedValue.floatValue(), resultSet.getFloat(3));
        Assertions.assertEquals(expectedValue.longValue(), resultSet.getLong(3));
        Assertions.assertEquals(expectedValue, resultSet.getObject(3));
        Assertions.assertEquals(String.valueOf(expectedValue), resultSet.getString(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(3));
    }

    @Test
    void testSmallRangedIntegerTypes() throws SQLException {
        testIntegerResultTypes(SparqlMockDataQuery.POSITIVE_INTEGER_QUERY, 5, 2);
        testIntegerResultTypes(SparqlMockDataQuery.NON_NEGATIVE_INTEGER_QUERY, 1, 2);
        testIntegerResultTypes(SparqlMockDataQuery.NEGATIVE_INTEGER_QUERY, -5, 2);
        testIntegerResultTypes(SparqlMockDataQuery.NON_POSITIVE_INTEGER_QUERY, -1, 2);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_POSITIVE_INTEGER_QUERY, 5, 3);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_NON_NEGATIVE_INTEGER_QUERY, 1, 3);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_NEGATIVE_INTEGER_QUERY, -5, 3);
        testIntegerResultTypes(SparqlMockDataQuery.CONSTRUCT_NON_POSITIVE_INTEGER_QUERY, -1, 3);
    }

    @Test
    void testDateType() throws SQLException {
        final ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.DATE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("1996-01-01", resultSet.getString(2));
        Assertions.assertEquals(java.sql.Date.valueOf("1996-01-01"), resultSet.getDate(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(2));
    }

    @Test
    void testConstructDateType() throws SQLException {
        final ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_DATE_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("1996-01-01", resultSet.getString(3));
        Assertions.assertEquals(java.sql.Date.valueOf("1996-01-01"), resultSet.getDate(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(3));
    }

    @Test
    void testTimeType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.TIME_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("22:10:10", resultSet.getString(2));
        Assertions.assertEquals(java.sql.Time.valueOf("22:10:10"), resultSet.getTime(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(2));
    }

    @Test
    void testConstructTimeType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_TIME_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("22:10:10", resultSet.getString(3));
        Assertions.assertEquals(java.sql.Time.valueOf("22:10:10"), resultSet.getTime(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(3));
    }

    @Test
    void testDateTimeType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.DATE_TIME_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020-01-01 00:10:10.0", resultSet.getString(2));
        Assertions.assertEquals(java.sql.Time.valueOf("00:10:10"), resultSet.getTime(2));
        Assertions.assertEquals(java.sql.Date.valueOf("2020-01-01"), resultSet.getDate(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(2));
    }

    @Test
    void testConstructDateTimeType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_DATE_TIME_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020-01-01 00:10:10.0", resultSet.getString(3));
        Assertions.assertEquals(java.sql.Time.valueOf("00:10:10"), resultSet.getTime(3));
        Assertions.assertEquals(java.sql.Date.valueOf("2020-01-01"), resultSet.getDate(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(3));
    }

    @Test
    void testDateTimeStampType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.DATE_TIME_STAMP_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020-01-01T06:10:10Z[UTC]", resultSet.getString(2));
        Assertions.assertEquals(java.sql.Time.valueOf("06:10:10"), resultSet.getTime(2));
        Assertions.assertEquals(java.sql.Date.valueOf("2020-01-01"), resultSet.getDate(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(2));
    }

    @Test
    void testConstructDateTimeStampType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_DATE_TIME_STAMP_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020-01-01T06:10:10Z[UTC]", resultSet.getString(3));
        Assertions.assertEquals(java.sql.Time.valueOf("06:10:10"), resultSet.getTime(3));
        Assertions.assertEquals(java.sql.Date.valueOf("2020-01-01"), resultSet.getDate(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(3));
    }

    @Test
    void testGYearType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.G_YEAR_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020", resultSet.getString(2));
        Assertions.assertEquals((byte) Integer.parseInt("2020"), resultSet.getByte(2));
        Assertions.assertEquals(new BigDecimal("2020"), resultSet.getBigDecimal(2));
        Assertions.assertEquals(Integer.parseInt("2020"), resultSet.getInt(2));
        Assertions.assertEquals(Short.parseShort("2020"), resultSet.getShort(2));
        Assertions.assertEquals(Long.parseLong("2020"), resultSet.getLong(2));
        Assertions.assertEquals(Double.parseDouble("2020"), resultSet.getLong(2));
        Assertions.assertEquals(Float.parseFloat("2020"), resultSet.getFloat(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(2));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(2));
    }

    @Test
    void testConstructGYearType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.CONSTRUCT_G_YEAR_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020", resultSet.getString(3));
        Assertions.assertEquals((byte) Integer.parseInt("2020"), resultSet.getByte(3));
        Assertions.assertEquals(new BigDecimal("2020"), resultSet.getBigDecimal(3));
        Assertions.assertEquals(Integer.parseInt("2020"), resultSet.getInt(3));
        Assertions.assertEquals(Short.parseShort("2020"), resultSet.getShort(3));
        Assertions.assertEquals(Long.parseLong("2020"), resultSet.getLong(3));
        Assertions.assertEquals(Double.parseDouble("2020"), resultSet.getLong(3));
        Assertions.assertEquals(Float.parseFloat("2020"), resultSet.getFloat(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(3));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(3));
    }

    @Test
    void testGMonthType() throws SQLException {
        testStringResultTypes(SparqlMockDataQuery.G_MONTH_QUERY, "--10", 2);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_G_MONTH_QUERY, "--10", 3);
    }

    @Test
    void testGDayType() throws SQLException {
        testStringResultTypes(SparqlMockDataQuery.G_DAY_QUERY, "---20", 2);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_G_DAY_QUERY, "---20", 3);
    }

    @Test
    void testGYearMonthType() throws SQLException {
        testStringResultTypes(SparqlMockDataQuery.G_YEAR_MONTH_QUERY, "2020-06", 2);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_G_YEAR_MONTH_QUERY, "2020-06", 3);
    }

    @Test
    void testGMonthDayType() throws SQLException {
        testStringResultTypes(SparqlMockDataQuery.G_MONTH_DAY_QUERY, "--06-01", 2);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_G_MONTH_DAY_QUERY, "--06-01", 3);
    }

    @Test
    void testDurationTypes() throws SQLException {
        testStringResultTypes(SparqlMockDataQuery.DURATION_QUERY, "P30D", 2);
        testStringResultTypes(SparqlMockDataQuery.YEAR_MONTH_DURATION_QUERY, "P2M", 2);
        testStringResultTypes(SparqlMockDataQuery.DAY_TIME_DURATION_QUERY, "P5D", 2);

        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_DURATION_QUERY, "P30D", 3);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_YEAR_MONTH_DURATION_QUERY, "P2M", 3);
        testStringResultTypes(SparqlMockDataQuery.CONSTRUCT_DAY_TIME_DURATION_QUERY, "P5D", 3);
    }

    @Test
    void testEmptySelectResult() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.EMPTY_SELECT_RESULT_QUERY);
        Assertions.assertThrows(SQLException.class, () -> resultSet.getString(2));
    }

    @Test
    void testAskQueryType() throws SQLException {
        final java.sql.ResultSet resultSet =
                statement.executeQuery(SparqlMockDataQuery.ASK_QUERY);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean(1));
        Assertions.assertEquals(String.valueOf(true), resultSet.getString(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBigDecimal(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getTimestamp(1));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getString(2));
        Assertions.assertFalse(resultSet.next());
    }

    // DESCRIBE and CONSTRUCT share ResultSet formats, so we just use this to test that this query type works
    @Test
    void testDescribeQueryType() throws SQLException {
        final java.sql.ResultSet resultSet = statement.executeQuery("DESCRIBE <http://somewhere/JohnSmith>");
        while (resultSet.next()) {
            Assertions.assertNotNull(resultSet.getString(1));
            Assertions.assertNotNull(resultSet.getString(2));
            Assertions.assertNotNull(resultSet.getString(3));
        }
    }

    @Test
    @Disabled
    void testQueryThroughRDFConnection() {
        // TODO: testing types through Jena RDF class, not through our driver, to be deleted after completing Sparql
        final RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                // Query only.
                .queryEndpoint("/query")
                .updateEndpoint("/update");

        // queries the database
        final Query query = QueryFactory.create("SELECT * { ?s ?p ?o } LIMIT 100");
        // final UpdateRequest update =
        //         UpdateFactory.create("PREFIX : <http://example/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
        //                 "INSERT DATA { :s :p \"2014-10-01T00:10:10\"^^xsd:dateTime }");

        // connects to database, updates the database, then query it
        try (final RDFConnection conn = builder.build()) {
            // conn.update(update);
            conn.queryResultSet(query, ResultSetFormatter::out);
        }

        final RDFConnection rdfConnection = builder.build();
        final QueryExecution queryExecution = rdfConnection.query(query);
        final org.apache.jena.query.ResultSet result = queryExecution.execSelect();

        final Map<Class<?>, JdbcType> sparqlJavaToJdbcMap = new HashMap<>();
        sparqlJavaToJdbcMap.put(String.class, JdbcType.VARCHAR);
        sparqlJavaToJdbcMap.put(Boolean.class, JdbcType.BIT);
        sparqlJavaToJdbcMap.put(byte[].class, JdbcType.VARCHAR);
        sparqlJavaToJdbcMap.put(Byte.class, JdbcType.TINYINT);
        sparqlJavaToJdbcMap.put(Short.class, JdbcType.SMALLINT);
        sparqlJavaToJdbcMap.put(Integer.class, JdbcType.INTEGER);
        sparqlJavaToJdbcMap.put(Long.class, JdbcType.BIGINT);
        // Should this be JdbcType.REAL?
        sparqlJavaToJdbcMap.put(Float.class, JdbcType.FLOAT);
        sparqlJavaToJdbcMap.put(Double.class, JdbcType.DOUBLE);
        sparqlJavaToJdbcMap.put(java.util.Date.class, JdbcType.DATE);
        sparqlJavaToJdbcMap.put(java.sql.Date.class, JdbcType.DATE);
        sparqlJavaToJdbcMap.put(Time.class, JdbcType.TIME);
        sparqlJavaToJdbcMap.put(Timestamp.class, JdbcType.TIMESTAMP);

        // BigInteger to BIGINT?
        sparqlJavaToJdbcMap.put(java.math.BigInteger.class, JdbcType.BIGINT);
        sparqlJavaToJdbcMap.put(java.math.BigDecimal.class, JdbcType.DECIMAL);

        final Map<Class<?>, Class<?>> sparqlToJavaMap = new HashMap<>();
        sparqlToJavaMap.put(org.apache.jena.datatypes.xsd.XSDDateTime.class, java.sql.Timestamp.class);

        while (result.hasNext()) {
            final QuerySolution querySolution = result.next();
            final RDFNode node = querySolution.get("o");
            System.out.println("|NODE CLASS                   | " + node.getClass());
            if (node.isLiteral()) {
                System.out.println("[--------------NEW ROW : LITERAL--------------]");
                final Literal literal = node.asLiteral();
                Class<?> javaClass = literal.getDatatype().getJavaClass();
                if (javaClass == null) {
                    javaClass = literal.getValue().getClass();
                }
                System.out.println("|FINAL CLASS                  | " + javaClass);
                System.out.println("|INSIDE JAVA-JDBC MAP?        | " +
                        sparqlJavaToJdbcMap.containsKey(javaClass));
                System.out.println("|INSIDE SPARQL-JAVA MAP?      | " +
                        sparqlToJavaMap.containsKey(javaClass));
                System.out.println("|VALUE                        | " + literal.getValue());
                System.out.println("|VALUE LEXICAL(String)        | " + literal.getLexicalForm());
                System.out.println("|LITERAL CLASS                | " + literal.getClass());
                System.out.println("|getValue().getClass()        | " + literal.getValue().getClass());
                System.out.println("|getDatatype()                | " + literal.getDatatype());
                System.out.println("|getDatatypeURI()             | " + literal.getDatatypeURI());
                System.out.println("|getDatatype().getClass()     | " + literal.getDatatype().getClass());
                System.out.println("|getDatatype().getJavaClass() | " + literal.getDatatype().getJavaClass());
            } else {
                System.out.println("[--------------NEW ROW : RESOURCE NODE--------------]");
                System.out.println(node.getClass() + ": " + node);
            }
        }

        System.out.println("[--------------DONE--------------]");
    }

    @Test
    @Disabled
    void testAskQueryThroughRDFConnection() {
        // TODO: testing types through Jena RDF class, not through our driver, to be deleted after completing Sparql
        final RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                // Query only.
                .queryEndpoint("/query")
                .updateEndpoint("/update");

        // queries the database
        final Query query = QueryFactory.create("ASK { }");
        final RDFConnection rdfConnection = builder.build();
        final QueryExecution queryExecution = rdfConnection.query(query);
        System.out.println(queryExecution.execAsk());
    }

    @Test
    @Disabled
    void testConstructQueryThroughRDFConnection() {
        // TODO: testing types through Jena RDF class, not through our driver, to be deleted after completing Sparql
        final RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                // Query only.
                .queryEndpoint("/query")
                .updateEndpoint("/update");

        // queries the database
        final Query query = QueryFactory.create("CONSTRUCT WHERE { ?s ?p ?o . }");
        final RDFConnection rdfConnection = builder.build();
        final QueryExecution queryExecution = rdfConnection.query(query);
        //System.out.println(queryExecution.execConstruct());
        final PeekIterator<Triple> triples = PeekIterator.create(queryExecution.execConstructTriples());
        while (triples.hasNext()) {
            // System.out.println(triples.next());
            System.out.println(triples.next());
        }
        //System.out.println(queryExecution.execConstructQuads());
    }

    @Test
    @Disabled
    void testDescribeQueryThroughRDFConnection() {
        // TODO: testing types through Jena RDF class, not through our driver, to be deleted after completing Sparql
        final RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                // Query only.
                .queryEndpoint("/query")
                .updateEndpoint("/update");

        // queries the database
        final Query query = QueryFactory.create("DESCRIBE ?o WHERE { ?s ?p ?o . }");
        final RDFConnection rdfConnection = builder.build();
        final QueryExecution queryExecution = rdfConnection.query(query);
        //System.out.println(queryExecution.execConstruct());
        final PeekIterator<Triple> triples = PeekIterator.create(queryExecution.execDescribeTriples());
        while (triples.hasNext()) {
            // System.out.println(triples.next());
            System.out.println(triples.next());
        }
        //System.out.println(queryExecution.execConstructQuads());
    }

    @Test
    @Disabled
    void testQueryThroughJDBCResult() throws SQLException {
        // TODO: to be deleted
        final String query =
                "SELECT ?x ?fname WHERE {?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  ?fname FILTER(ISNUMERIC(?fname))}";
        final SparqlSelectResultSet result = (SparqlSelectResultSet) statement.executeQuery(query);
        printJenaResultSetOut(query);
        System.out.println(result.getResultMetadata().getColumnName(2));
        System.out.println(result.next());

        // next() increments the RowIndex everytime it is called (see ResultSet)
        // Assertions.assertTrue(result.next());

        while (result.next()) {
            System.out.println("[--------------NEW ROW--------------]");
            System.out.println("|STRING VALUE  | " + result.getConvertedValue(3));
            System.out.println("|CONVERT VALUE | " + result.getConvertedValue(3));
            System.out.println("|VALUE CLASS   | " + result.getConvertedValue(3).getClass());
        }

        Assertions.assertFalse(result.next());
    }

}
