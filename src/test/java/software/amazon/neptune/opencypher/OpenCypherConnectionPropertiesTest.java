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

package software.amazon.neptune.opencypher;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.ConnectionPropertiesTestBase;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Test for OpenCypherConnectionProperties.
 */
class OpenCypherConnectionPropertiesTest extends ConnectionPropertiesTestBase {
    private OpenCypherConnectionProperties connectionProperties;

    protected void assertDoesNotThrowOnNewConnectionProperties(final Properties properties) {
        Assertions.assertDoesNotThrow(() -> {
            connectionProperties = new OpenCypherConnectionProperties(properties);
        });
    }

    protected void assertThrowsOnNewConnectionProperties(final Properties properties) {
        Assertions.assertThrows(SQLException.class,
                () -> connectionProperties = new OpenCypherConnectionProperties(properties));
    }

    protected <T> void assertPropertyValueEqualsToExpected(final String key, final T expectedValue) {
        Assertions.assertEquals(expectedValue, connectionProperties.get(key));
    }

    @Test
    void testDefaultValues() throws SQLException {
        connectionProperties = new OpenCypherConnectionProperties();
        Assertions.assertEquals("", connectionProperties.getEndpoint());
        Assertions.assertEquals(OpenCypherConnectionProperties.DEFAULT_CONNECTION_TIMEOUT_MILLIS,
                connectionProperties.getConnectionTimeoutMillis());
        Assertions.assertEquals(OpenCypherConnectionProperties.DEFAULT_CONNECTION_RETRY_COUNT,
                connectionProperties.getConnectionRetryCount());
        Assertions.assertEquals(OpenCypherConnectionProperties.DEFAULT_CONNECTION_POOL_SIZE,
                connectionProperties.getConnectionPoolSize());
        Assertions
                .assertEquals(OpenCypherConnectionProperties.DEFAULT_AUTH_SCHEME, connectionProperties.getAuthScheme());
        Assertions.assertEquals(OpenCypherConnectionProperties.DEFAULT_USE_ENCRYPTION,
                connectionProperties.getUseEncryption());
        Assertions.assertEquals("", connectionProperties.getRegion());
    }

    @Test
    void testApplicationName() throws SQLException {
        testStringPropertyViaConstructor(
                OpenCypherConnectionProperties.APPLICATION_NAME_KEY);

        final String testValue = "test application name";
        connectionProperties = new OpenCypherConnectionProperties();
        connectionProperties.setApplicationName(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getApplicationName());
    }

    @Test
    void testEndpoint() throws SQLException {
        testStringPropertyViaConstructor(
                OpenCypherConnectionProperties.ENDPOINT_KEY,
                DEFAULT_EMPTY_STRING);

        final String testValue = "test endpoint";
        connectionProperties = new OpenCypherConnectionProperties();
        connectionProperties.setEndpoint(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getEndpoint());
    }

    @Test
    void testRegion() throws SQLException {
        Properties initProperties = new Properties();
        initProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reset to None

        testStringPropertyViaConstructor(
                initProperties,
                OpenCypherConnectionProperties.REGION_KEY,
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
    void testAwsCredentialsProviderClass() throws SQLException {
        testStringPropertyViaConstructor(
                OpenCypherConnectionProperties.AWS_CREDENTIALS_PROVIDER_CLASS_KEY);

        connectionProperties = new OpenCypherConnectionProperties();
        final String testValue = "test AwsCredentialsProviderClass";
        connectionProperties.setAwsCredentialsProviderClass(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getAwsCredentialsProviderClass());
    }

    @Test
    void testCustomCredentialsFilePath() throws SQLException {
        testStringPropertyViaConstructor(
                OpenCypherConnectionProperties.CUSTOM_CREDENTIALS_FILE_PATH_KEY);

        connectionProperties = new OpenCypherConnectionProperties();
        final String testValue = "test CustomCredentialsFilePath";
        connectionProperties.setCustomCredentialsFilePath(testValue);
        Assertions.assertEquals(testValue, connectionProperties.getCustomCredentialsFilePath());
    }

    @Test
    void testConnectionTimeout() throws SQLException {
        testIntegerPropertyViaConstructor(
                OpenCypherConnectionProperties.CONNECTION_TIMEOUT_MILLIS_KEY,
                OpenCypherConnectionProperties.DEFAULT_CONNECTION_TIMEOUT_MILLIS);

        connectionProperties = new OpenCypherConnectionProperties();
        connectionProperties.setConnectionTimeoutMillis(10);
        Assertions.assertEquals(10, connectionProperties.getConnectionTimeoutMillis());
    }

    @Test
    void testConnectionRetryCount() throws SQLException {
        testIntegerPropertyViaConstructor(
                OpenCypherConnectionProperties.CONNECTION_RETRY_COUNT_KEY,
                OpenCypherConnectionProperties.DEFAULT_CONNECTION_RETRY_COUNT);

        connectionProperties = new OpenCypherConnectionProperties();
        connectionProperties.setConnectionRetryCount(10);
        Assertions.assertEquals(10, connectionProperties.getConnectionRetryCount());
    }

    @Test
    void testConnectionPoolSize() throws SQLException {
        testIntegerPropertyViaConstructor(
                OpenCypherConnectionProperties.CONNECTION_POOL_SIZE_KEY,
                OpenCypherConnectionProperties.DEFAULT_CONNECTION_POOL_SIZE);

        connectionProperties = new OpenCypherConnectionProperties();
        connectionProperties.setConnectionPoolSize(10);
        Assertions.assertEquals(10, connectionProperties.getConnectionPoolSize());
    }

    @Test
    void testAuthScheme() throws SQLException {
        testAuthSchemeViaConstructor();

        connectionProperties = new OpenCypherConnectionProperties();
        connectionProperties.setAuthScheme(AuthScheme.None);
        Assertions.assertEquals(AuthScheme.None, connectionProperties.getAuthScheme());
    }

    @Test
    void testUseEncryption() throws SQLException {
        Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reset to None
        testBooleanPropertyViaConstructor(
                properties,
                OpenCypherConnectionProperties.USE_ENCRYPTION_KEY,
                OpenCypherConnectionProperties.DEFAULT_USE_ENCRYPTION);

        // new set of properties
        properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reset to None
        assertDoesNotThrowOnNewConnectionProperties(properties);
        final ImmutableList<Boolean> boolValues = ImmutableList.of(true, false);
        for (final Boolean boolValue : boolValues) {
            connectionProperties.setUseEncryption(boolValue);
            Assertions.assertEquals(boolValue, connectionProperties.getUseEncryption());
        }
    }
}
