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

package software.amazon.neptune.gremlin;

import com.google.common.collect.ImmutableList;
import io.netty.handler.ssl.SslContext;
import org.apache.log4j.Level;
import org.apache.tinkerpop.gremlin.driver.LoadBalancingStrategy;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.ConnectionPropertiesTestBase;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.mock;

/**
 * Test for GremlinConnectionProperties.
 */
class GremlinConnectionPropertiesTest extends ConnectionPropertiesTestBase {
    private GremlinConnectionProperties connectionProperties;
    private int randomIntValue;

    protected void assertDoesNotThrowOnNewConnectionProperties(final Properties properties) {
        Assertions.assertDoesNotThrow(() -> {
            connectionProperties = new GremlinConnectionProperties(properties);
        });
    }

    protected void assertThrowsOnNewConnectionProperties(final Properties properties) {
        Assertions.assertThrows(SQLException.class,
                () -> connectionProperties = new GremlinConnectionProperties(properties));
    }

    protected <T> void assertPropertyValueEqualsToExpected(final String key, final T expectedValue) {
        Assertions.assertEquals(expectedValue, connectionProperties.get(key));
    }

    @BeforeEach
    void beforeEach() {
        randomIntValue = HelperFunctions.randomPositiveIntValue(1000);
    }

    @Test
    void testDefaultValues() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(GremlinConnectionProperties.DEFAULT_LOG_LEVEL, connectionProperties.getLogLevel());
        Assertions.assertEquals(GremlinConnectionProperties.DEFAULT_CONNECTION_TIMEOUT_MILLIS, connectionProperties.getConnectionTimeoutMillis());
        Assertions.assertEquals(GremlinConnectionProperties.DEFAULT_CONNECTION_RETRY_COUNT, connectionProperties.getConnectionRetryCount());
        Assertions.assertEquals(GremlinConnectionProperties.DEFAULT_AUTH_SCHEME, connectionProperties.getAuthScheme());
        Assertions.assertEquals("", connectionProperties.getContactPoint());
        Assertions.assertEquals(GremlinConnectionProperties.DEFAULT_PATH, connectionProperties.getPath());
        Assertions.assertEquals(GremlinConnectionProperties.DEFAULT_PORT, connectionProperties.getPort());
        Assertions.assertEquals(GremlinConnectionProperties.DEFAULT_ENABLE_SSL, connectionProperties.getEnableSsl());
    }

    @Test
    void testApplicationName() throws SQLException {
        testStringPropertyViaConstructor(
                GremlinConnectionProperties.APPLICATION_NAME_KEY);

        final String testValue = "test application name";
        connectionProperties = new GremlinConnectionProperties();
        connectionProperties.setApplicationName(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getApplicationName());
    }

    @Test
    void testLogLevel() throws SQLException {
        testLogLevelSettingViaConstructor();

        connectionProperties = new GremlinConnectionProperties();
        connectionProperties.setLogLevel(Level.ERROR);
        Assertions.assertEquals(Level.ERROR, connectionProperties.getLogLevel());
    }

    @Test
    void testConnectionTimeout() throws SQLException {
        testIntegerPropertyViaConstructor(
                GremlinConnectionProperties.CONNECTION_TIMEOUT_MILLIS_KEY,
                GremlinConnectionProperties.DEFAULT_CONNECTION_TIMEOUT_MILLIS);

        connectionProperties = new GremlinConnectionProperties();
        connectionProperties.setConnectionTimeoutMillis(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getConnectionTimeoutMillis());
    }

    @Test
    void testConnectionRetryCount() throws SQLException {
        testIntegerPropertyViaConstructor(
                GremlinConnectionProperties.CONNECTION_RETRY_COUNT_KEY,
                GremlinConnectionProperties.DEFAULT_CONNECTION_RETRY_COUNT);

        connectionProperties = new GremlinConnectionProperties();
        connectionProperties.setConnectionRetryCount(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getConnectionRetryCount());
    }

    @Test
    void testAuthScheme() throws SQLException {
        testAuthSchemeViaConstructor();

        connectionProperties = new GremlinConnectionProperties();
        connectionProperties.setAuthScheme(AuthScheme.None);
        Assertions.assertEquals(AuthScheme.None, connectionProperties.getAuthScheme());
    }

    @Test
    void testContactPoint() throws SQLException {
        testStringPropertyViaConstructor(
                GremlinConnectionProperties.CONTACT_POINT_KEY,
                ConnectionPropertiesTestBase.DEFAULT_EMPTY_STRING);

        final String testValue = "test contact point";
        connectionProperties = new GremlinConnectionProperties();
        connectionProperties.setContactPoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getContactPoint());
    }

    @Test
    void testPath() throws SQLException {
        testStringPropertyViaConstructor(
                GremlinConnectionProperties.PATH_KEY,
                GremlinConnectionProperties.DEFAULT_PATH);

        final String testValue = "test path";
        connectionProperties = new GremlinConnectionProperties();
        connectionProperties.setPath(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getPath());
    }

    @Test
    void testPort() throws SQLException {
        testIntegerPropertyViaConstructor(
                GremlinConnectionProperties.PORT_KEY,
                GremlinConnectionProperties.DEFAULT_PORT);

        final int testValue = 12345;
        connectionProperties = new GremlinConnectionProperties();
        connectionProperties.setPort(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getPort());
    }

    @Test
    void testSerializerObject() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNotNull(connectionProperties.getSerializerObject());

        final Serializers serializer = Serializers.GRAPHBINARY_V1D0;
        Assertions.assertDoesNotThrow(
                () -> connectionProperties.setSerializer(serializer)
        );
        Assertions.assertTrue(connectionProperties.isSerializerObject());
        Assertions.assertFalse(connectionProperties.isSerializerString());
        Assertions.assertEquals(serializer, connectionProperties.getSerializerObject());
    }

    @Test
    void testSerializerString() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        final String serializer = "test serializer";
        Assertions.assertDoesNotThrow(
                () -> connectionProperties.setSerializer(serializer)
        );

        Assertions.assertTrue(connectionProperties.isSerializerString());
        Assertions.assertFalse(connectionProperties.isSerializerObject());
        Assertions.assertEquals(serializer, connectionProperties.getSerializerString());
    }

    @Test
    void testEnableSsl() throws SQLException {
        Properties initProperties = new Properties();
        initProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reset to None
        testBooleanPropertyViaConstructor(
                initProperties,
                GremlinConnectionProperties.ENABLE_SSL_KEY,
                GremlinConnectionProperties.DEFAULT_ENABLE_SSL);

        initProperties = new Properties();
        initProperties.put(GremlinConnectionProperties.ENABLE_SSL_KEY, true);
        initProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4);
        assertDoesNotThrowOnNewConnectionProperties(initProperties);

        initProperties = new Properties();
        initProperties.put(GremlinConnectionProperties.ENABLE_SSL_KEY, false);
        initProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4);
        assertThrowsOnNewConnectionProperties(initProperties);

        initProperties = new Properties();
        initProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None);
        assertDoesNotThrowOnNewConnectionProperties(initProperties);
        final ImmutableList<Boolean> boolValues = ImmutableList.of(true, false);
        for (final Boolean boolValue : boolValues) {
            connectionProperties.setEnableSsl(boolValue);
            Assertions.assertEquals(boolValue, connectionProperties.getEnableSsl());
        }
    }

    @Test
    void testSslContext() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getSslContext());

        final SslContext sslContext = mock(SslContext.class);
        Assertions.assertDoesNotThrow(
                () -> connectionProperties.setSslContext(sslContext)
        );
        Assertions.assertEquals(sslContext, connectionProperties.getSslContext());
    }

    @Test
    void testSslEnabledProtocols() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getSslEnabledProtocols());

        final List<String> sslEnabledProtocols = ImmutableList.of("test sslEnabledProtocols");
        Assertions.assertDoesNotThrow(
                () -> connectionProperties.setSslEnabledProtocols(sslEnabledProtocols)
        );
        Assertions.assertEquals(sslEnabledProtocols, connectionProperties.getSslEnabledProtocols());
    }

    @Test
    void testSslCipherSuites() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getSslCipherSuites());

        final List<String> sslCipherSuites = ImmutableList.of("test sslCipherSuites");
        Assertions.assertDoesNotThrow(
                () -> connectionProperties.setSslCipherSuites(sslCipherSuites)
        );
        Assertions.assertEquals(sslCipherSuites, connectionProperties.getSslCipherSuites());
    }

    @Test
    void testSslSkipCertValidation() throws SQLException {
        testBooleanPropertyViaConstructor(
                GremlinConnectionProperties.SSL_SKIP_VALIDATION_KEY,
                GremlinConnectionProperties.DEFAULT_SSL_SKIP_VALIDATION);

        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(DEFAULT_FALSE, connectionProperties.getSslSkipCertValidation());
        final ImmutableList<Boolean> boolValues = ImmutableList.of(true, false);
        for (final Boolean boolValue : boolValues) {
            connectionProperties.setSslSkipCertValidation(boolValue);
            Assertions.assertEquals(boolValue, connectionProperties.getSslSkipCertValidation());
        }
    }

    @Test
    void testKeyStore() throws SQLException {
        final String testValue = "test key store";
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getKeyStore());
        connectionProperties.setKeyStore(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getKeyStore());
    }

    @Test
    void testKeyStorePassword() throws SQLException {
        final String testValue = "test key store password";
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getKeyStorePassword());
        connectionProperties.setKeyStorePassword(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getKeyStorePassword());
    }

    @Test
    void testKeyStoreType() throws SQLException {
        final String testValue = "test key store type";
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getKeyStoreType());
        connectionProperties.setKeyStoreType(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getKeyStoreType());
    }

    @Test
    void testTrustStore() throws SQLException {
        final String testValue = "test trust store";
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getTrustStore());
        connectionProperties.setTrustStore(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getTrustStore());
    }

    @Test
    void testTrustStorePassword() throws SQLException {
        final String testValue = "test trust store password";
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getTrustStorePassword());
        connectionProperties.setTrustStorePassword(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getTrustStorePassword());
    }

    @Test
    void testTrustStoreType() throws SQLException {
        final String testValue = "test trust store type";
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getTrustStoreType());
        connectionProperties.setTrustStoreType(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getTrustStoreType());
    }

    @Test
    void testNioPoolSize() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getNioPoolSize());
        connectionProperties.setNioPoolSize(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getNioPoolSize());
    }

    @Test
    void testWorkerPoolSize() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getWorkerPoolSize());
        connectionProperties.setWorkerPoolSize(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getWorkerPoolSize());
    }

    @Test
    void testMaxConnectionPoolSize() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getMaxConnectionPoolSize());
        connectionProperties.setMaxConnectionPoolSize(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getMaxConnectionPoolSize());
    }

    @Test
    void testMinConnectionPoolSize() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getMinConnectionPoolSize());
        connectionProperties.setMinConnectionPoolSize(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getMinConnectionPoolSize());
    }

    @Test
    void testMaxInProcessPerConnection() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getMaxInProcessPerConnection());
        connectionProperties.setMaxInProcessPerConnection(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getMaxInProcessPerConnection());
    }

    @Test
    void testMinInProcessPerConnection() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getMinInProcessPerConnection());
        connectionProperties.setMinInProcessPerConnection(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getMinInProcessPerConnection());
    }

    @Test
    void testMaxSimultaneousUsagePerConnection() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getMaxSimultaneousUsagePerConnection());
        connectionProperties.setMaxSimultaneousUsagePerConnection(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getMaxSimultaneousUsagePerConnection());
    }

    @Test
    void testMinSimultaneousUsagePerConnection() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getMinSimultaneousUsagePerConnection());
        connectionProperties.setMinSimultaneousUsagePerConnection(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getMinSimultaneousUsagePerConnection());
    }

    @Test
    void testChannelizerGeneric() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getChannelizerGeneric());

        final Class<?> channelizer = Object.class;
        Assertions.assertDoesNotThrow(
                () -> connectionProperties.setChannelizer(channelizer)
        );
        Assertions.assertTrue(connectionProperties.isChannelizerGeneric());
        Assertions.assertFalse(connectionProperties.isChannelizerString());
        Assertions.assertEquals(channelizer, connectionProperties.getChannelizerGeneric());
    }

    @Test
    void testChannelizerString() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getChannelizerString());

        final String channelizer = "test channelizer";
        Assertions.assertDoesNotThrow(
                () -> connectionProperties.setChannelizer(channelizer)
        );
        Assertions.assertTrue(connectionProperties.isChannelizerString());
        Assertions.assertFalse(connectionProperties.isChannelizerGeneric());
        Assertions.assertEquals(channelizer, connectionProperties.getChannelizerString());
    }

    @Test
    void testKeepAliveInterval() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getKeepAliveInterval());
        connectionProperties.setKeepAliveInterval(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getKeepAliveInterval());
    }

    @Test
    void testResultIterationBatchSize() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getResultIterationBatchSize());
        connectionProperties.setResultIterationBatchSize(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getResultIterationBatchSize());
    }

    @Test
    void testMaxWaitForConnection() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getMaxWaitForConnection());
        connectionProperties.setMaxWaitForConnection(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getMaxWaitForConnection());
    }

    @Test
    void testMaxWaitForClose() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getMaxWaitForClose());
        connectionProperties.setMaxWaitForClose(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getMaxWaitForClose());
    }

    @Test
    void testMaxContentLength() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getMaxContentLength());
        connectionProperties.setMaxContentLength(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getMaxContentLength());
    }

    @Test
    void testValidationRequest() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getValidationRequest());

        final String validationRequest = "test validationRequest";
        connectionProperties.setValidationRequest(validationRequest);
        Assertions.assertEquals(validationRequest, connectionProperties.getValidationRequest());
    }

    @Test
    void testReconnectInterval() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertEquals(NO_DEFAULT_INT, connectionProperties.getReconnectInterval());
        connectionProperties.setReconnectInterval(randomIntValue);
        Assertions.assertEquals(randomIntValue, connectionProperties.getReconnectInterval());
    }

    @Test
    void testLoadBalancingStrategy() throws SQLException {
        connectionProperties = new GremlinConnectionProperties();
        Assertions.assertNull(connectionProperties.getLoadBalancingStrategy());

        final LoadBalancingStrategy strategy = mock(LoadBalancingStrategy.class);
        Assertions.assertDoesNotThrow(
                () -> connectionProperties.setLoadBalancingStrategy(strategy)
        );
        Assertions.assertEquals(strategy, connectionProperties.getLoadBalancingStrategy());
    }
}
