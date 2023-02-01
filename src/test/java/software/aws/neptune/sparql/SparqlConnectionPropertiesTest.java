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

package software.aws.neptune.sparql;

import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.aws.neptune.jdbc.helpers.HelperFunctions;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import java.sql.SQLException;
import java.util.Properties;

public class SparqlConnectionPropertiesTest {
    private static final String HOSTNAME = "http://localhost";
    private static final int PORT = 8182;
    private static final String ENDPOINT = "mock";
    private SparqlConnectionProperties connectionProperties;
    private int randomIntValue;

    protected void assertDoesNotThrowOnNewConnectionProperties(final Properties properties) {
        Assertions.assertDoesNotThrow(() -> {
            // Since we have added the check for service region and IAMSigV4 is set by default, we need to add a mock
            // region property here in case the system running these tests does not have SERVICE_REGION variable set.
            properties.put("serviceRegion", "mock-region");
            connectionProperties = new SparqlConnectionProperties(properties);
        });
    }

    protected void assertThrowsOnNewConnectionProperties(final Properties properties) {
        Assertions.assertThrows(SQLException.class,
                () -> connectionProperties = new SparqlConnectionProperties(properties));
    }

    // set the DESTINATION properties properly to avoid throws on tests not related to the exception
    private void setInitialDestinationProperty(final SparqlConnectionProperties connectionProperties)
            throws SQLException {
        connectionProperties.setEndpoint(HOSTNAME);
        connectionProperties.setPort(PORT);
        connectionProperties.setDataset(ENDPOINT);
    }

    @BeforeEach
    void beforeEach() {
        randomIntValue = HelperFunctions.randomPositiveIntValue(1000);
    }

    @Test
    void testDefaultValues() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertEquals("", connectionProperties.getDataset());
        Assertions.assertEquals(SparqlConnectionProperties.DEFAULT_CONNECTION_TIMEOUT_MILLIS,
                connectionProperties.getConnectionTimeoutMillis());
        Assertions.assertEquals(SparqlConnectionProperties.DEFAULT_CONNECTION_RETRY_COUNT,
                connectionProperties.getConnectionRetryCount());
        Assertions
                .assertEquals(SparqlConnectionProperties.DEFAULT_AUTH_SCHEME, connectionProperties.getAuthScheme());
        Assertions.assertEquals(SparqlConnectionProperties.DEFAULT_SERVICE_REGION, connectionProperties.getServiceRegion());
    }

    @Test
    void testApplicationName() throws SQLException {
        final String testValue = "test application name";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setApplicationName(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getApplicationName());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testConnectionTimeout() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setConnectionTimeoutMillis(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getConnectionTimeoutMillis());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testConnectionRetryCount() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setConnectionRetryCount(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getConnectionRetryCount());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testContactPoint() throws SQLException {
        final String testValue = "test contact point";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setEndpoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getEndpoint());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testEndpoint() throws SQLException {
        final String testValue = "test endpoint";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setDataset(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getDataset());

        // will throw because only Endpoint is set
        assertThrowsOnNewConnectionProperties(connectionProperties);

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testQueryEndpoint() throws SQLException {
        final String testValue = "test query endpoint";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setQueryEndpoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getQueryEndpoint());

        // will throw because only QueryEndpoint is set
        assertThrowsOnNewConnectionProperties(connectionProperties);

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testPort() throws SQLException {
        final int testValue = 12345;
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setPort(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getPort());

        // will throw because only Port is set
        assertThrowsOnNewConnectionProperties(connectionProperties);

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testAuthScheme() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAuthScheme(AuthScheme.None);
        Assertions.assertEquals(AuthScheme.None, connectionProperties.getAuthScheme());
        System.out.println("region is: " + connectionProperties.getServiceRegion());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testRegion() throws SQLException {
        connectionProperties = new SparqlConnectionProperties();

        connectionProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set to None
        connectionProperties.setServiceRegion("ca-central-1");
        Assertions.assertEquals("ca-central-1", connectionProperties.getServiceRegion());

        connectionProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set to IAMSigV4
        connectionProperties.setServiceRegion("us-east-1");
        Assertions.assertEquals("us-east-1", connectionProperties.getServiceRegion());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testConcurrentModificationExceptionHttpClient() throws SQLException {
        final HttpClient testClient = HttpClientBuilder.create().build();
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertNull(connectionProperties.getHttpClient());

        connectionProperties.setHttpClient(testClient);
        Assertions.assertEquals(testClient, connectionProperties.getHttpClient());

        connectionProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None);
        Assertions.assertEquals(AuthScheme.None, connectionProperties.getAuthScheme());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testHttpClient() throws SQLException {
        final HttpClient testClient = HttpClientBuilder.create().build();
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertNull(connectionProperties.getHttpClient());

        connectionProperties.setHttpClient(testClient);
        Assertions.assertEquals(testClient, connectionProperties.getHttpClient());

        connectionProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None);
        Assertions.assertEquals(AuthScheme.None, connectionProperties.getAuthScheme());

        final Properties testProperties = new Properties();
        testProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None);
        testProperties.put(SparqlConnectionProperties.ENDPOINT_KEY, "http://localhost");
        testProperties.put(SparqlConnectionProperties.PORT_KEY, 8182);
        testProperties.put(SparqlConnectionProperties.DATASET_KEY, "mock");
        testProperties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, "query");
        testProperties.put(SparqlConnectionProperties.HTTP_CLIENT_KEY, testClient);
        Assertions.assertEquals(testClient, testProperties.get(SparqlConnectionProperties.HTTP_CLIENT_KEY));

        assertDoesNotThrowOnNewConnectionProperties(testProperties);
    }

    @Test
    void testHttpClientWithSigV4Auth() {
        final HttpClient testClient = HttpClientBuilder.create().build();
        Assertions.assertNotNull(testClient);

        final Properties testProperties = new Properties();
        testProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4);
        testProperties.put(SparqlConnectionProperties.ENDPOINT_KEY, "http://localhost");
        testProperties.put(SparqlConnectionProperties.PORT_KEY, 8182);
        testProperties.put(SparqlConnectionProperties.DATASET_KEY, "mock");
        testProperties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, "query");
        testProperties.put(SparqlConnectionProperties.HTTP_CLIENT_KEY, testClient);
        Assertions.assertEquals(testClient, testProperties.get(SparqlConnectionProperties.HTTP_CLIENT_KEY));

        assertThrowsOnNewConnectionProperties(testProperties);
    }

    @Test
    void testHttpContext() throws SQLException {
        final HttpContext testContext = new HttpClientContext();
        connectionProperties = new SparqlConnectionProperties();
        Assertions.assertNull(connectionProperties.getHttpContext());
        connectionProperties.setHttpContext(testContext);
        Assertions.assertEquals(testContext, connectionProperties.getHttpContext());

        final Properties testProperties = new Properties();
        testProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None);
        testProperties.put(SparqlConnectionProperties.ENDPOINT_KEY, "http://localhost");
        testProperties.put(SparqlConnectionProperties.PORT_KEY, 8182);
        testProperties.put(SparqlConnectionProperties.DATASET_KEY, "mock");
        testProperties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, "query");
        testProperties.put(SparqlConnectionProperties.HTTP_CONTEXT_KEY, testContext);
        Assertions.assertEquals(testContext, testProperties.get(SparqlConnectionProperties.HTTP_CONTEXT_KEY));

        assertDoesNotThrowOnNewConnectionProperties(testProperties);
    }

    @Test
    void testAcceptHeaderAskQuery() throws SQLException {
        final String testValue = "test accept header ask query";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderAskQuery(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderAskQuery());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testAcceptHeaderDataset() throws SQLException {
        final String testValue = "test accept header graph";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderDataset(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderDataset());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testAcceptHeaderQuery() throws SQLException {
        final String testValue = "test accept header query";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderQuery(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderQuery());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testAcceptHeaderSelectQuery() throws SQLException {
        final String testValue = "test accept header select query";
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setAcceptHeaderSelectQuery(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAcceptHeaderSelectQuery());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testParseCheckSparql() throws SQLException {
        final boolean testValue = true;
        connectionProperties = new SparqlConnectionProperties();
        connectionProperties.setParseCheckSparql(testValue);
        Assertions.assertTrue(connectionProperties.getParseCheckSparql());

        // the constructor test with DESTINATION properties properly set to avoid throws
        setInitialDestinationProperty(connectionProperties);
        final Properties properties = new Properties();
        properties.putAll(connectionProperties);
        assertDoesNotThrowOnNewConnectionProperties(properties);
    }

    @Test
    void testChangeAuthSchemeToNone() throws SQLException {
        // Use encryption is always set because Neptune only supports encrypted connections on SPARQL.
        final Properties properties = new Properties();
        properties.put("authScheme", "IAMSigV4");
        properties.put("endpointURL", "mock");
        properties.put("port", "1234");
        properties.put("serviceRegion", "mock-region");
        connectionProperties = new SparqlConnectionProperties(properties);
        Assertions.assertEquals(connectionProperties.getAuthScheme(), AuthScheme.IAMSigV4);
        Assertions.assertDoesNotThrow(() -> connectionProperties.setAuthScheme(AuthScheme.None));
        Assertions.assertEquals(connectionProperties.getAuthScheme(), AuthScheme.None);
    }

    @Test
    void testChangeAuthSchemeToIAMSigV4() throws SQLException {
        final Properties properties = new Properties();
        properties.put("authScheme", "None");
        properties.put("endpointURL", "mock");
        properties.put("port", "1234");
        connectionProperties = new SparqlConnectionProperties(properties);
        Assertions.assertEquals(connectionProperties.getAuthScheme(), AuthScheme.None);
        Assertions.assertDoesNotThrow(() -> connectionProperties.setAuthScheme(AuthScheme.IAMSigV4));
        Assertions.assertEquals(connectionProperties.getAuthScheme(), AuthScheme.IAMSigV4);
    }
}
