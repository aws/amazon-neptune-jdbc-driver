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

package software.amazon.neptune.sparql;

import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.riot.web.HttpOp;
import org.apache.log4j.Level;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.ConnectionPropertiesTestBase;
import java.sql.SQLException;
import java.util.Properties;

public class SparqlConnectionPropertiesTest extends ConnectionPropertiesTestBase {
    private SparqlConnectionProperties connectionProperties;
    private int randomIntValue;

    @Override
    protected void assertDoesNotThrowOnNewConnectionProperties(final Properties properties) {
        Assertions.assertDoesNotThrow(() -> {
            connectionProperties = new SparqlConnectionProperties(properties);
        });
    }

    @Override
    protected void assertThrowsOnNewConnectionProperties(final Properties properties) {
        Assertions.assertThrows(SQLException.class,
                () -> connectionProperties = new SparqlConnectionProperties(properties));
    }

    @Override
    protected <T> void assertPropertyValueEqualsToExpected(final String key, final T expectedValue) {
        Assertions.assertEquals(expectedValue, connectionProperties.get(key));
    }

    @BeforeEach
    void beforeEach() {
        randomIntValue = HelperFunctions.randomPositiveIntValue(1000);
    }

    // TODO: edge cases for auth, etc

    @Test
    void testDefaultValues() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertEquals("", connectionProperties.getEndpoint());
        Assertions.assertEquals(SparqlConnectionProperties.DEFAULT_LOG_LEVEL, connectionProperties.getLogLevel());
        Assertions.assertEquals(SparqlConnectionProperties.DEFAULT_CONNECTION_TIMEOUT_MILLIS,
                connectionProperties.getConnectionTimeoutMillis());
        Assertions.assertEquals(SparqlConnectionProperties.DEFAULT_CONNECTION_RETRY_COUNT,
                connectionProperties.getConnectionRetryCount());
        Assertions
                .assertEquals(SparqlConnectionProperties.DEFAULT_AUTH_SCHEME, connectionProperties.getAuthScheme());
        Assertions.assertEquals("", connectionProperties.getRegion());
    }

    @Test
    void testApplicationName() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.APPLICATION_NAME_KEY);

        final String testValue = "test application name";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setApplicationName(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getApplicationName());
    }

    @Test
    void testLogLevel() throws SQLException {
        testLogLevelSettingViaConstructor();

        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setLogLevel(Level.ERROR);
        Assertions.assertEquals(Level.ERROR, connectionProperties.getLogLevel());
    }

    @Test
    void testConnectionTimeout() throws SQLException {
        testIntegerPropertyViaConstructor(
                SparqlConnectionProperties.CONNECTION_TIMEOUT_MILLIS_KEY,
                SparqlConnectionProperties.DEFAULT_CONNECTION_TIMEOUT_MILLIS);

        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setConnectionTimeoutMillis(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getConnectionTimeoutMillis());
    }

    @Test
    void testConnectionRetryCount() throws SQLException {
        testIntegerPropertyViaConstructor(
                SparqlConnectionProperties.CONNECTION_RETRY_COUNT_KEY,
                SparqlConnectionProperties.DEFAULT_CONNECTION_RETRY_COUNT);

        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setConnectionRetryCount(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getConnectionRetryCount());
    }

    @Test
    void testContactPoint() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.CONTACT_POINT_KEY,
                ConnectionPropertiesTestBase.DEFAULT_EMPTY_STRING);

        final String testValue = "test contact point";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setContactPoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getContactPoint());
    }

    @Test
    void testEndpoint() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.ENDPOINT_KEY,
                DEFAULT_EMPTY_STRING);

        final String testValue = "test endpoint";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setEndpoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getEndpoint());
    }

    @Test
    void testQueryEndpoint() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.QUERY_ENDPOINT_KEY,
                DEFAULT_EMPTY_STRING);

        final String testValue = "test query endpoint";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setQueryEndpoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getQueryEndpoint());
    }

    @Test
    void testPort() throws SQLException {
        testIntegerPropertyViaConstructor(
                SparqlConnectionProperties.PORT_KEY,
                SparqlConnectionProperties.DEFAULT_PORT);

        final int testValue = 12345;
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setPort(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getPort());
    }

    @Test
    void testAuthScheme() throws SQLException {
        testAuthSchemeViaConstructor();

        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAuthScheme(AuthScheme.None);
        Assertions.assertEquals(AuthScheme.None, connectionProperties.getAuthScheme());
        System.out.println("region is: " + connectionProperties.getRegion());
    }

    @Test
    void testRegion() throws SQLException {
        Properties initProperties = new Properties();
        initProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reset to None

        testStringPropertyViaConstructor(
                initProperties,
                SparqlConnectionProperties.REGION_KEY,
                DEFAULT_EMPTY_STRING);

        initProperties = new Properties();
        initProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reset to None
        assertDoesNotThrowOnNewConnectionProperties(initProperties);

        final String testValue = "test region";
        connectionProperties.setRegion(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getRegion());

        initProperties = new Properties();
        initProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set to IAMSigV4
        assertDoesNotThrowOnNewConnectionProperties(initProperties);

        final String serviceRegion = System.getenv().get("SERVICE_REGION");
        Assertions.assertNotNull(serviceRegion);
        connectionProperties.setRegion(serviceRegion);
        Assertions.assertEquals(serviceRegion, connectionProperties.getRegion());
    }

    @Test
    void testHttpClient() throws SQLException {
        final HttpClient testClient = HttpOp.createDefaultHttpClient();
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertNull(connectionProperties.getHttpClient());
        connectionProperties.setHttpClient(testClient);
        Assertions.assertEquals(testClient, connectionProperties.getHttpClient());
    }

    @Test
    void testHttpClientWithSigV4() throws SQLException {
        final HttpClient testClient = HttpOp.createDefaultHttpClient();
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertNotNull(testClient);
        Assertions.assertNull(connectionProperties.getHttpClient());
        connectionProperties.setAuthScheme(AuthScheme.IAMSigV4); // set default to None
        connectionProperties.setContactPoint("http://localhost");
        connectionProperties.setPort(3030);
        connectionProperties.setEndpoint("mock");
        connectionProperties.setQueryEndpoint("query");
        connectionProperties.setHttpClient(testClient);
        Assertions.assertEquals(testClient, connectionProperties.getHttpClient());

        final Properties testProperties = new Properties();
        testProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4);
        testProperties.put(SparqlConnectionProperties.CONTACT_POINT_KEY, "http://localhost");
        testProperties.put(SparqlConnectionProperties.PORT_KEY, 3030);
        testProperties.put(SparqlConnectionProperties.ENDPOINT_KEY, "mock");
        testProperties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, "query");
        testProperties.put(SparqlConnectionProperties.HTTP_CLIENT_KEY, testClient);

        Assertions.assertThrows(SQLException.class,
                () -> new SparqlConnection(new SparqlConnectionProperties(testProperties)));
    }

    @Test
    void testHttpContext() throws SQLException {
        final HttpContext testContext = new HttpClientContext();
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertNull(connectionProperties.getHttpContext());
        connectionProperties.setHttpContext(testContext);
        Assertions.assertEquals(testContext, connectionProperties.getHttpContext());
    }

    @Test
    void testAcceptHeaderAskQuery() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.ACCEPT_HEADER_ASK_QUERY_KEY);

        final String testValue = "test accept header ask query";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderAskQuery(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderAskQuery());
    }

    @Test
    void testAcceptHeaderDataset() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.ACCEPT_HEADER_DATASET_KEY);

        final String testValue = "test accept header graph";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderDataset(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderDataset());
    }

    @Test
    void testAcceptHeaderGraph() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.ACCEPT_HEADER_GRAPH_KEY);

        final String testValue = "test accept header graph";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderGraph(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderGraph());
    }

    @Test
    void testAcceptHeaderQuery() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.ACCEPT_HEADER_QUERY_KEY);

        final String testValue = "test accept header query";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderQuery(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderQuery());
    }

    @Test
    void testAcceptHeaderSelectQuery() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.ACCEPT_HEADER_SELECT_QUERY_KEY);

        final String testValue = "test accept header select query";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderSelectQuery(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderSelectQuery());
    }

    @Test
    void testGspEndpoint() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.GSP_ENDPOINT_KEY);

        final String testValue = "test gsp endpoint";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setGspEndpoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getGspEndpoint());
    }

    @Test
    void testParseCheckSparql() throws SQLException {
        testBooleanPropertyViaConstructor(
                SparqlConnectionProperties.PARSE_CHECK_SPARQL_KEY);

        final boolean testValue = true;
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setParseCheckSparql(testValue);
        Assertions.assertTrue(connectionProperties.getParseCheckSparql());
    }

    @Test
    void testQuadsFormat() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.QUADS_FORMAT_KEY);

        final String testValue = "test quads format";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setQuadsFormat(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getQuadsFormat());
    }


    @Test
    void testTriplesFormat() throws SQLException {
        testStringPropertyViaConstructor(
                SparqlConnectionProperties.TRIPLES_FORMAT_KEY);

        final String testValue = "test triples format";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setTriplesFormat(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getTriplesFormat());
    }
}
