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

package software.amazon.neptune.opencypher;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.AuthScheme;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Test for OpenCypherConnectionProperties.
 */
class OpenCypherConnectionPropertiesTest {
    private OpenCypherConnectionProperties connectionProperties;

    @Test
    void testDefaultValues() throws SQLException {
        connectionProperties = new OpenCypherConnectionProperties();
        Assertions.assertEquals("", connectionProperties.getEndpoint());
        Assertions.assertEquals(OpenCypherConnectionProperties.DEFAULT_LOG_LEVEL, connectionProperties.getLogLevel());
        Assertions.assertEquals(OpenCypherConnectionProperties.DEFAULT_CONNECTION_TIMEOUT_MILLIS, connectionProperties.getConnectionTimeoutMillis());
        Assertions.assertEquals(OpenCypherConnectionProperties.DEFAULT_CONNECTION_RETRY_COUNT, connectionProperties.getConnectionRetryCount());
        Assertions.assertEquals(OpenCypherConnectionProperties.DEFAULT_AUTH_SCHEME, connectionProperties.getAuthScheme());
        Assertions.assertEquals(OpenCypherConnectionProperties.DEFAULT_USE_ENCRYPTION, connectionProperties.getUseEncryption());
        Assertions.assertEquals("", connectionProperties.getRegion());
    }

    @Test
    void testLogLevelSetting() throws SQLException {
        final List<String> validLogLevels = ImmutableList.of(
                "", "Off", "FATAL", "error", "InFo", "dEbug", "TRACE", "All");
        final List<String> invalidLogLevels = ImmutableList.of(
                "something", "5");

        final Properties properties = new Properties();

        // Verify valid property value doesn't throw error.
        for (final String validValue : validLogLevels) {
            // Set property through constructor.
            properties.put(OpenCypherConnectionProperties.LOG_LEVEL_KEY, validValue);
            Assertions.assertDoesNotThrow(() -> {
                connectionProperties = new OpenCypherConnectionProperties(properties);
            });
        }

        // Verify invalid property value throws error.
        for (final String invalidValue : invalidLogLevels) {
            // Set property through constructor.
            properties.setProperty(OpenCypherConnectionProperties.LOG_LEVEL_KEY, invalidValue);
            Assertions.assertThrows(SQLException.class,
                    () -> connectionProperties = new OpenCypherConnectionProperties(properties));
        }
    }

    @Test
    void testConnectionTimeout() throws SQLException {
        final List<String> validConnectionTimeouts = ImmutableList.of(
                "0", "5", "10000");
        final List<String> invalidConnectionTimeouts = ImmutableList.of(
                "-1", "blah", String.valueOf((long)Integer.MAX_VALUE + 1000));

        final Properties properties = new Properties();

        // Verify empty string is set as default value.
        properties.put(OpenCypherConnectionProperties.CONNECTION_TIMEOUT_MILLIS_KEY, "");
        Assertions.assertDoesNotThrow(() -> {
            connectionProperties = new OpenCypherConnectionProperties(properties);
        });
        Assertions.assertEquals(OpenCypherConnectionProperties.DEFAULT_CONNECTION_TIMEOUT_MILLIS,
                connectionProperties.getConnectionTimeoutMillis());

        // Verify valid property value doesn't throw error.
        for (final String validValue : validConnectionTimeouts) {
            // Set property through constructor.
            properties.put(OpenCypherConnectionProperties.CONNECTION_TIMEOUT_MILLIS_KEY, validValue);
            Assertions.assertDoesNotThrow(() -> {
                connectionProperties = new OpenCypherConnectionProperties(properties);
            });
            Assertions.assertEquals(Integer.parseInt(validValue), connectionProperties.getConnectionTimeoutMillis());
        }

        // Verify invalid property value throws error.
        for (final String invalidValue : invalidConnectionTimeouts) {
            // Set property through constructor.
            properties.setProperty(OpenCypherConnectionProperties.CONNECTION_TIMEOUT_MILLIS_KEY, invalidValue);
            Assertions.assertThrows(SQLException.class,
                    () -> connectionProperties = new OpenCypherConnectionProperties(properties));
        }
    }

    @Test
    void testConnectionRetryCount() throws SQLException {
        final List<String> validConnectionTimeouts = ImmutableList.of(
                "0", "5", "10000");
        final List<String> invalidConnectionTimeouts = ImmutableList.of(
                "-1", "blah", String.valueOf((long)Integer.MAX_VALUE + 1000));

        final Properties properties = new Properties();

        // Verify empty string is set as default value.
        properties.put(OpenCypherConnectionProperties.CONNECTION_RETRY_COUNT_KEY, "");
        Assertions.assertDoesNotThrow(() -> {
            connectionProperties = new OpenCypherConnectionProperties(properties);
        });
        Assertions.assertEquals(OpenCypherConnectionProperties.DEFAULT_CONNECTION_RETRY_COUNT,
                connectionProperties.getConnectionRetryCount());

        // Verify valid property value doesn't throw error.
        for (final String validValue : validConnectionTimeouts) {
            // Set property through constructor.
            properties.put(OpenCypherConnectionProperties.CONNECTION_RETRY_COUNT_KEY, validValue);
            Assertions.assertDoesNotThrow(() -> {
                connectionProperties = new OpenCypherConnectionProperties(properties);
            });
            Assertions.assertEquals(Integer.parseInt(validValue), connectionProperties.getConnectionRetryCount());
        }

        // Verify invalid property value throws error.
        for (final String invalidValue : invalidConnectionTimeouts) {
            // Set property through constructor.
            properties.setProperty(OpenCypherConnectionProperties.CONNECTION_RETRY_COUNT_KEY, invalidValue);
            Assertions.assertThrows(SQLException.class,
                    () -> connectionProperties = new OpenCypherConnectionProperties(properties));
        }
    }

    @Test
    void testAuthScheme() throws SQLException {
        final List<String> validAuthSchemes = ImmutableList.of(
                "NONE", "none", "IAMSigV4", "iamSIGV4", "IAMRole", "IaMRoLe");
        final List<String> invalidAuthSchemes = ImmutableList.of(
                "-1;", "100;", "46hj7;", "foo;");

        final Properties properties = new Properties();

        // Verify empty string is set as default value.
        properties.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, "");
        Assertions.assertDoesNotThrow(() -> {
            connectionProperties = new OpenCypherConnectionProperties(properties);
        });
        Assertions.assertEquals(AuthScheme.None, connectionProperties.getAuthScheme());

        // Verify valid property value is set.
        for (final String validValue : validAuthSchemes) {
            // Convert string to enum.
            Assertions.assertNotNull(
                    AuthScheme.fromString(validValue)
            );
            // Set property through constructor.
            properties.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, validValue);
            Assertions.assertDoesNotThrow(() -> {
                connectionProperties = new OpenCypherConnectionProperties(properties);
            });
            Assertions.assertEquals(AuthScheme.fromString(validValue), connectionProperties.getAuthScheme());
            // Set property directly.
            connectionProperties = new OpenCypherConnectionProperties();
            connectionProperties.setAuthScheme(AuthScheme.fromString(validValue));
            Assertions.assertEquals(AuthScheme.fromString(validValue), connectionProperties.getAuthScheme());
        }

        // Verify invalid property value throws error.
        for (final String invalidValue : invalidAuthSchemes) {
            // Convert string to enum.
            Assertions.assertNull(
                    AuthScheme.fromString(invalidValue)
            );
            // Set property through constructor.
            properties.setProperty(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, invalidValue);
            Assertions.assertThrows(SQLException.class,
                    () -> connectionProperties = new OpenCypherConnectionProperties(properties));
        }
    }

    @Test
    void testUseEncryption() throws SQLException {
        final List<String> validTrueValues = ImmutableList.of(
                "", "   ", "1", "true", "TRUE", "tRue");
        final List<String> validFalseValues = ImmutableList.of(
                "0", "false", "FALSE", "FaLSe");
        final List<String> invalidValues = ImmutableList.of(
                "-1;", "100;", "46hj7;", "foo;");

        final Properties properties = new Properties();

        // Verify valid TRUE property value is set.
        for (final String validValue : validTrueValues) {
            // Set property through constructor.

            properties.put(OpenCypherConnectionProperties.USE_ENCRYPTION_KEY, validValue);
            Assertions.assertDoesNotThrow(() -> {
                connectionProperties = new OpenCypherConnectionProperties(properties);
            });
            Assertions.assertTrue(connectionProperties.getUseEncryption());
        }

        // Verify valid FALSE property value is set.
        // 'AuthScheme' must be NONE if UseEncryption is FALSE.
        for (final String validValue : validFalseValues) {
            // Set properties through constructor.
            properties.put(OpenCypherConnectionProperties.USE_ENCRYPTION_KEY, validValue);
            properties.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None);
            Assertions.assertDoesNotThrow(() -> {
                connectionProperties = new OpenCypherConnectionProperties(properties);
            });
            Assertions.assertFalse(connectionProperties.getUseEncryption());
        }

        // Verify invalid property value throws error.
        for (final String invalidValue : invalidValues) {
            // Set properties through constructor.
            properties.setProperty(OpenCypherConnectionProperties.USE_ENCRYPTION_KEY, invalidValue);
            Assertions.assertThrows(SQLException.class,
                    () -> connectionProperties = new OpenCypherConnectionProperties(properties));
        }
    }
}
