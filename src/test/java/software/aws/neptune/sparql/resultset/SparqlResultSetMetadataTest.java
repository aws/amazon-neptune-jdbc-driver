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

package software.aws.neptune.sparql.resultset;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFormatter;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class SparqlResultSetMetadataTest {
    // TODO: Test cases may change when we address type promotion in performance ticket.
    private static final String HOSTNAME = "http://localhost";
    private static final String DATASET = "mock";
    private static final String QUERY_ENDPOINT = "query";
    private static final int PORT = SparqlMockServer.port(); // Mock server dynamically generates port
    private static final String STRING_QUERY_ONE_COLUMN =
            "SELECT ?fname WHERE {?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  ?fname}";
    private static final String STRING_QUERY_TWO_COLUMN =
            "SELECT ?x ?fname WHERE {?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  ?fname}";
    private static final String STRING_QUERY_THREE_COLUMN =
            "SELECT ?s ?x ?fname WHERE {?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  ?fname}";
    private static final List<SparqlResultSetMetadataTest.MetadataTestHelper> METADATA_TEST_HELPER =
            ImmutableList.of(
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.ALL_DATA_TWO_COLUMNS_QUERY,
                            0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                            String.class.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.STRING_QUERY,
                            0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                            XSDDatatype.XSDstring.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.LONG_QUERY,
                            20, 19, 0, false, true, java.sql.Types.BIGINT, Long.class.getTypeName(),
                            XSDDatatype.XSDlong.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.INTEGER_SMALL_QUERY,
                            20, 19, 0, false, true, java.sql.Types.BIGINT, java.math.BigInteger.class.getName(),
                            XSDDatatype.XSDinteger.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.DECIMAL_QUERY,
                            25, 15, 15, false, true, java.sql.Types.DECIMAL, java.math.BigDecimal.class.getName(),
                            XSDDatatype.XSDdecimal.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.SHORT_QUERY,
                            20, 19, 0, false, true, java.sql.Types.SMALLINT, Short.class.getTypeName(),
                            XSDDatatype.XSDshort.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.BOOL_QUERY,
                            1, 1, 0, false, false, java.sql.Types.BIT, Boolean.class.getTypeName(),
                            XSDDatatype.XSDboolean.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.DOUBLE_QUERY,
                            25, 15, 15, false, true, java.sql.Types.DOUBLE, Double.class.getTypeName(),
                            XSDDatatype.XSDdouble.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.FLOAT_QUERY,
                            25, 15, 6, false, true, java.sql.Types.REAL, Float.class.getTypeName(),
                            XSDDatatype.XSDfloat.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.DATE_QUERY,
                            24, 24, 0, false, false, java.sql.Types.DATE, java.sql.Date.class.getTypeName(),
                            XSDDatatype.XSDdate.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.TIME_QUERY,
                            24, 24, 0, false, false, java.sql.Types.TIME, java.sql.Time.class.getTypeName(),
                            XSDDatatype.XSDtime.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.DATE_TIME_QUERY,
                            24, 24, 0, false, false, java.sql.Types.TIMESTAMP, java.sql.Timestamp.class.getTypeName(),
                            XSDDatatype.XSDdateTime.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.DATE_TIME_STAMP_QUERY,
                            24, 24, 0, false, false, java.sql.Types.TIMESTAMP, java.sql.Timestamp.class.getTypeName(),
                            XSDDatatype.XSDdateTimeStamp.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.DURATION_QUERY,
                            0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                            XSDDatatype.XSDduration.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.PREDICATE_QUERY,
                            0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                            org.apache.jena.graph.Node_URI.class.toString())
            );
    private static final List<SparqlResultSetMetadataTest.MetadataTestHelper> CONSTRUCT_METADATA_TEST_HELPER =
            ImmutableList.of(
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY,
                            0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                            XSDDatatype.XSDstring.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.CONSTRUCT_LONG_QUERY,
                            20, 19, 0, false, true, java.sql.Types.BIGINT, Long.class.getTypeName(),
                            XSDDatatype.XSDlong.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(
                            SparqlMockDataQuery.CONSTRUCT_INTEGER_SMALL_QUERY,
                            20, 19, 0, false, true, java.sql.Types.BIGINT, java.math.BigInteger.class.getName(),
                            XSDDatatype.XSDinteger.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.CONSTRUCT_DECIMAL_QUERY,
                            25, 15, 15, false, true, java.sql.Types.DECIMAL, java.math.BigDecimal.class.getName(),
                            XSDDatatype.XSDdecimal.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.CONSTRUCT_SHORT_QUERY,
                            20, 19, 0, false, true, java.sql.Types.SMALLINT, Short.class.getTypeName(),
                            XSDDatatype.XSDshort.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.CONSTRUCT_BOOL_QUERY,
                            1, 1, 0, false, false, java.sql.Types.BIT, Boolean.class.getTypeName(),
                            XSDDatatype.XSDboolean.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.CONSTRUCT_DOUBLE_QUERY,
                            25, 15, 15, false, true, java.sql.Types.DOUBLE, Double.class.getTypeName(),
                            XSDDatatype.XSDdouble.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.CONSTRUCT_FLOAT_QUERY,
                            25, 15, 6, false, true, java.sql.Types.REAL, Float.class.getTypeName(),
                            XSDDatatype.XSDfloat.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.CONSTRUCT_DATE_QUERY,
                            24, 24, 0, false, false, java.sql.Types.DATE, java.sql.Date.class.getTypeName(),
                            XSDDatatype.XSDdate.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.CONSTRUCT_TIME_QUERY,
                            24, 24, 0, false, false, java.sql.Types.TIME, java.sql.Time.class.getTypeName(),
                            XSDDatatype.XSDtime.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.CONSTRUCT_DATE_TIME_QUERY,
                            24, 24, 0, false, false, java.sql.Types.TIMESTAMP, java.sql.Timestamp.class.getTypeName(),
                            XSDDatatype.XSDdateTime.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(
                            SparqlMockDataQuery.CONSTRUCT_DATE_TIME_STAMP_QUERY,
                            24, 24, 0, false, false, java.sql.Types.TIMESTAMP, java.sql.Timestamp.class.getTypeName(),
                            XSDDatatype.XSDdateTimeStamp.toString()),
                    new SparqlResultSetMetadataTest.MetadataTestHelper(SparqlMockDataQuery.CONSTRUCT_DURATION_QUERY,
                            0, 256, 0, true, false, java.sql.Types.VARCHAR, String.class.getTypeName(),
                            XSDDatatype.XSDduration.toString())
            );
    private static java.sql.Connection connection;
    private static RDFConnectionRemoteBuilder rdfConnBuilder;

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

        // insert into the database here
        rdfConnBuilder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                // Query only.
                .queryEndpoint("/query");

        // load dataset in
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
    }

    @AfterEach
    void shutdown() throws SQLException {
        connection.close();
    }

    ResultSetMetaData getResultSetMetaData(final String query) throws SQLException {
        final java.sql.ResultSet resultSet = connection.createStatement().executeQuery(query);
        return resultSet.getMetaData();
    }

    @Test
    void testGetColumnCount() throws SQLException {
        Assertions.assertEquals(1, getResultSetMetaData(STRING_QUERY_ONE_COLUMN).getColumnCount());
        Assertions.assertEquals(2, getResultSetMetaData(STRING_QUERY_TWO_COLUMN).getColumnCount());
        Assertions.assertEquals(3, getResultSetMetaData(STRING_QUERY_THREE_COLUMN).getColumnCount());

        Assertions.assertEquals(3, getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).getColumnCount());

        Assertions.assertEquals(1, getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getColumnCount());
    }

    @Test
    void testGetColumnDisplaySize() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getDisplaySize(),
                    getResultSetMetaData(helper.getQuery()).getColumnDisplaySize(2), "For query: " + helper.getQuery());
        }
    }

    @Test
    void testConstructGetColumnDisplaySize() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : CONSTRUCT_METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getDisplaySize(),
                    getResultSetMetaData(helper.getQuery()).getColumnDisplaySize(3), "For query: " + helper.getQuery());
        }
    }

    @Test
    void testAskGetColumnDisplaySize() throws SQLException {
        Assertions.assertEquals(1, getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getColumnDisplaySize(1));
    }

    @Test
    void testGetPrecision() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getPrecision(), getResultSetMetaData(helper.getQuery()).getPrecision(2),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testConstructGetPrecision() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : CONSTRUCT_METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getPrecision(), getResultSetMetaData(helper.getQuery()).getPrecision(3),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testAskGetPrecision() throws SQLException {
        Assertions.assertEquals(1, getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getPrecision(1));
    }

    @Test
    void testGetScale() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getScale(), getResultSetMetaData(helper.getQuery()).getScale(2),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testConstructGetScale() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : CONSTRUCT_METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getScale(), getResultSetMetaData(helper.getQuery()).getScale(3),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testAskGetScale() throws SQLException {
        Assertions.assertEquals(0, getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getScale(1));
    }

    @Test
    void testIsAutoIncrement() throws SQLException {
        Assertions.assertFalse(getResultSetMetaData(STRING_QUERY_TWO_COLUMN).isAutoIncrement(1));
        Assertions.assertFalse(getResultSetMetaData(STRING_QUERY_TWO_COLUMN).isAutoIncrement(2));
        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).isAutoIncrement(1));
        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).isAutoIncrement(2));
        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).isAutoIncrement(3));

        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).isAutoIncrement(1));
    }

    @Test
    void testIsCaseSensitive() throws SQLException {
        Assertions.assertTrue(getResultSetMetaData(STRING_QUERY_TWO_COLUMN).isCaseSensitive(2));
        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.INT_QUERY).isCaseSensitive(2));

        Assertions.assertTrue(getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_INT_QUERY).isCaseSensitive(2));
        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_INT_QUERY).isCaseSensitive(3));

        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).isCaseSensitive(1));
    }

    @Test
    void testIsSearchable() throws SQLException {
        Assertions.assertFalse(getResultSetMetaData(STRING_QUERY_TWO_COLUMN).isSearchable(2));

        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).isSearchable(3));

        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).isSearchable(1));
    }

    @Test
    void testIsCurrency() throws SQLException {
        Assertions.assertFalse(getResultSetMetaData(STRING_QUERY_TWO_COLUMN).isCurrency(2));
        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.INT_QUERY).isCurrency(2));

        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_INT_QUERY).isCurrency(2));
        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_INT_QUERY).isCurrency(3));

        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).isCurrency(1));
    }

    @Test
    void testIsNullable() throws SQLException {
        Assertions.assertEquals(ResultSetMetaData.columnNullableUnknown,
                getResultSetMetaData(STRING_QUERY_TWO_COLUMN).isNullable(2));
        Assertions.assertEquals(ResultSetMetaData.columnNullableUnknown,
                getResultSetMetaData(SparqlMockDataQuery.INT_QUERY).isNullable(2));
        Assertions.assertEquals(ResultSetMetaData.columnNullableUnknown,
                getResultSetMetaData(SparqlMockDataQuery.BOOL_QUERY).isNullable(2));

        Assertions.assertEquals(ResultSetMetaData.columnNullableUnknown,
                getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_INT_QUERY).isNullable(3));
        Assertions.assertEquals(ResultSetMetaData.columnNullableUnknown,
                getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_BOOL_QUERY).isNullable(3));

        Assertions.assertEquals(ResultSetMetaData.columnNullableUnknown,
                getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).isNullable(1));
    }

    @Test
    void testIsSigned() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.isSigned(), getResultSetMetaData(helper.getQuery()).isSigned(2),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testConstructIsSigned() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : CONSTRUCT_METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.isSigned(), getResultSetMetaData(helper.getQuery()).isSigned(3),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testAskIsSigned() throws SQLException {
        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).isSigned(1));
    }

    @Test
    void testGetColumnLabel() throws SQLException {
        Assertions.assertEquals("fname", getResultSetMetaData(STRING_QUERY_ONE_COLUMN).getColumnName(1));
        Assertions.assertEquals("Subject",
                getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).getColumnName(1));
        Assertions.assertEquals("Ask", getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getColumnName(1));
    }

    @Test
    void testGetColumnName() throws SQLException {
        Assertions.assertEquals("fname", getResultSetMetaData(STRING_QUERY_ONE_COLUMN).getColumnName(1));
        Assertions.assertEquals("Subject",
                getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).getColumnName(1));
        Assertions.assertEquals("Ask", getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getColumnName(1));
    }

    @Test
    void testGetColumnType() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getJdbcType(), getResultSetMetaData(helper.getQuery()).getColumnType(2),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testConstructGetColumnType() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : CONSTRUCT_METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getJdbcType(), getResultSetMetaData(helper.getQuery()).getColumnType(3),
                    "For query: " + helper.getQuery());
        }
    }

    @Test
    void testAskGetColumnType() throws SQLException {
        Assertions
                .assertEquals(java.sql.Types.BIT, getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getColumnType(1));
    }

    @Test
    void testGetColumnTypeName() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getColumnJenaClassName(),
                    getResultSetMetaData(helper.getQuery()).getColumnTypeName(2), "For query: " + helper.getQuery());
        }
    }

    @Test
    void testConstructGetColumnTypeName() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : CONSTRUCT_METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getColumnJenaClassName(),
                    getResultSetMetaData(helper.getQuery()).getColumnTypeName(3), "For query: " + helper.getQuery());
        }
    }

    @Test
    void testAskGetColumnTypeName() throws SQLException {
        Assertions.assertEquals(Boolean.class.getName(),
                getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getColumnTypeName(1));
    }

    @Test
    void testGetColumnClassName() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getColumnJavaClassName(),
                    getResultSetMetaData(helper.getQuery()).getColumnClassName(2), "For query: " + helper.getQuery());
        }
    }

    @Test
    void testConstructGetColumnClassName() throws SQLException {
        for (final SparqlResultSetMetadataTest.MetadataTestHelper helper : CONSTRUCT_METADATA_TEST_HELPER) {
            Assertions.assertEquals(helper.getColumnJavaClassName(),
                    getResultSetMetaData(helper.getQuery()).getColumnClassName(3), "For query: " + helper.getQuery());
        }
    }

    @Test
    void testAskGetColumnClassName() throws SQLException {
        Assertions.assertEquals(Boolean.class.getName(),
                getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getColumnClassName(1));
    }

    @Test
    void testIsReadOnly() throws SQLException {
        Assertions.assertTrue(getResultSetMetaData(STRING_QUERY_ONE_COLUMN).isReadOnly(1));
        Assertions.assertTrue(getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).isReadOnly(3));
        Assertions.assertTrue(getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).isReadOnly(1));
    }

    @Test
    void testIsWritable() throws SQLException {
        Assertions.assertFalse(getResultSetMetaData(STRING_QUERY_ONE_COLUMN).isWritable(1));
        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).isWritable(3));
        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).isWritable(1));
    }

    @Test
    void testIsDefinitelyWritable() throws SQLException {
        Assertions.assertFalse(getResultSetMetaData(STRING_QUERY_ONE_COLUMN).isDefinitelyWritable(1));
        Assertions
                .assertFalse(getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).isDefinitelyWritable(3));
        Assertions.assertFalse(getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).isDefinitelyWritable(1));
    }

    @Test
    void testGetTableName() throws SQLException {
        Assertions.assertEquals("", getResultSetMetaData(STRING_QUERY_ONE_COLUMN).getTableName(1));
        Assertions.assertEquals("", getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).getTableName(3));
        Assertions.assertEquals("", getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getTableName(1));
    }

    @Test
    void testGetSchemaName() throws SQLException {
        Assertions.assertEquals("", getResultSetMetaData(STRING_QUERY_ONE_COLUMN).getSchemaName(1));
        Assertions.assertEquals("", getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).getSchemaName(3));
        Assertions.assertEquals("", getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getSchemaName(1));
    }

    @Test
    void testGetCatalogName() throws SQLException {
        Assertions.assertEquals("", getResultSetMetaData(STRING_QUERY_ONE_COLUMN).getCatalogName(1));
        Assertions.assertEquals("", getResultSetMetaData(SparqlMockDataQuery.CONSTRUCT_STRING_QUERY).getCatalogName(3));
        Assertions.assertEquals("", getResultSetMetaData(SparqlMockDataQuery.ASK_QUERY).getCatalogName(1));
    }

    @AllArgsConstructor
    @Getter
    static class MetadataTestHelper {
        private final String query;
        private final int displaySize;
        private final int precision;
        private final int scale;
        private final boolean caseSensitive;
        private final boolean signed;
        private final int jdbcType;
        private final String columnJavaClassName;
        private final String columnJenaClassName;
    }
}
