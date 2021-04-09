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

import org.apache.log4j.Level;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.neptune.ConnectionPropertiesTestBase;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Test for GremlinConnectionProperties.
 */
class GremlinConnectionPropertiesTest extends ConnectionPropertiesTestBase {
    private GremlinConnectionProperties connectionProperties;

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
    void testLogLevel() throws SQLException {
        testLogLevelSettingViaConstructor();

        assertDoesNotThrowOnNewConnectionProperties(new Properties());
        connectionProperties.setLogLevel(Level.ERROR);
        Assertions.assertEquals(Level.ERROR, connectionProperties.getLogLevel());
    }

    @Test
    void testConnectionTimeout() throws SQLException {
        testIntegerPropertyViaConstructor(
                GremlinConnectionProperties.CONNECTION_TIMEOUT_MILLIS_KEY,
                GremlinConnectionProperties.DEFAULT_CONNECTION_TIMEOUT_MILLIS, true);

        assertDoesNotThrowOnNewConnectionProperties(new Properties());
        connectionProperties.setConnectionTimeoutMillis(10);
        Assertions.assertEquals(10, connectionProperties.getConnectionTimeoutMillis());
    }

    @Test
    void testConnectionRetryCount() throws SQLException {
        testIntegerPropertyViaConstructor(
                GremlinConnectionProperties.CONNECTION_RETRY_COUNT_KEY,
                GremlinConnectionProperties.DEFAULT_CONNECTION_RETRY_COUNT, true);

        assertDoesNotThrowOnNewConnectionProperties(new Properties());
        connectionProperties.setConnectionRetryCount(10);
        Assertions.assertEquals(10, connectionProperties.getConnectionRetryCount());
    }

    @Test
    void testAuthScheme() throws SQLException {
        testAuthSchemeViaConstructor();

        assertDoesNotThrowOnNewConnectionProperties(new Properties());
        connectionProperties.setAuthScheme(AuthScheme.None);
        Assertions.assertEquals(AuthScheme.None, connectionProperties.getAuthScheme());
    }
}
