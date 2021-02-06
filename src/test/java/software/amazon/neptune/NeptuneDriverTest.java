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

package software.amazon.neptune;

import com.google.common.collect.ImmutableList;
import org.apache.log4j.LogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.opencypher.OpenCypherConnection;
import software.amazon.neptune.opencypher.mock.MockOpenCypherDatabase;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Test for NeptuneDriver Object.
 */
public class NeptuneDriverTest {
    private static MockOpenCypherDatabase database;
    private static String validEndpoint;
    private final List<String> invalidUrls = ImmutableList.of(
            "jbdc:neptune:opencyher://;", "jdbc:netune:opencyher://;", "jdbc:neptune:openyher://;",
            "jdbc:neptune:opencyher//;", "jdbc:neptune:opencypher:/");
    private final List<String> languages = ImmutableList.of("opencypher");
    private final List<Boolean> semicolons = ImmutableList.of(true, false);
    private final List<String> properties = ImmutableList.of("user=username", "user=username;password=password");
    private java.sql.Driver driver;

    private static String createValidUrl(final String language, final String properties,
                          final boolean trailingSemicolon) {
        String url = String.format("jdbc:neptune:%s://%s", language, validEndpoint);
        if (!properties.isEmpty()) {
            url += String.format(";%s", properties);
        }
        if (trailingSemicolon) {
            url += ";";
        }
        return url;
    }

    private static String appendProperty(final String url, final String property, final boolean trailingSemicolon) {
        String returnUrl = url;
        if (!property.isEmpty()) {
            returnUrl += String.format("%s", property);
        }
        if (trailingSemicolon) {
            returnUrl += ";";
        }
        return returnUrl;
    }

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() {
        database = MockOpenCypherDatabase.builder("localhost", NeptuneDriverTest.class.getName()).build();
        validEndpoint = String.format("bolt://%s:%d", "localhost", database.getPort());
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        database.shutdown();
    }

    @BeforeEach
    void initialize() {
        driver = new NeptuneDriver();
    }

    @Test
    void testAcceptsUrl() throws SQLException {
        for (final String language : languages) {
            for (final String property : properties) {
                for (final Boolean semicolon : semicolons) {
                    final String url = createValidUrl(language, property, semicolon);
                }
            }
        }
        for (final String url : invalidUrls) {
            Assertions.assertFalse(driver.acceptsURL(url));
        }
    }

    @Test
    void testConnect() throws SQLException {
        for (final String language : languages) {
            for (final String property : properties) {
                for (final Boolean semicolon : semicolons) {
                    final String validUrl = createValidUrl(language, property, semicolon);
                    Assertions.assertTrue(driver.connect(validUrl, new Properties()) instanceof OpenCypherConnection);
                }
            }
        }
        final String validUrl = createValidUrl("opencypher", "", false);
        Assertions.assertNull(driver.connect(validUrl, null));

        for (final String invalidUrl : invalidUrls) {
             Assertions.assertNull(driver.connect(invalidUrl, new Properties()));
        }
        Assertions.assertNull(driver.connect(null, new Properties()));

    }

    @Test
    void testLogLevelSetting() throws SQLException {
        Assertions.assertEquals(ConnectionProperties.DEFAULT_LOG_LEVEL, LogManager.getRootLogger().getLevel());
        final List<String> validLogLevels = ImmutableList.of(
                "", "logLevel=;", "logLevel=FATAL;", "LogLevel= error", "LOGleVel = InFo ;", "LOGLEVEL=dEbug", "logLEVEL=TRACE;");
        final List<String> invalidLogLevels = ImmutableList.of(
                "logLevel=something;", "LogLevel=5;");
        for (final String language : languages) {
            for (final String property : properties) {
                final String url = createValidUrl(language, property, true);
                for (final String logLevel : validLogLevels) {
                    final String validUrl = appendProperty(url, logLevel, false);
                    Assertions.assertTrue(driver.connect(validUrl, new Properties()) instanceof OpenCypherConnection);
                }
                for (final String invalidLogLevel : invalidLogLevels) {
                    final String invalidUrl = appendProperty(url, invalidLogLevel, false);
                    Assertions.assertNull(driver.connect(invalidUrl, new Properties()));
                }
            }
        }
        // Reset logging so that it doesn't affect other tests.
        LogManager.getRootLogger().setLevel(ConnectionProperties.DEFAULT_LOG_LEVEL);
    }

    @Test
    void testConnectionTimeout() throws SQLException {
        final List<String> validConnectionTimeouts = ImmutableList.of(
                "", "connectionTimeout=;", "connectionTimeout=0;", "connectionTimeout= 5", "ConnectionTimeouT = 10000 ;");
        @SuppressWarnings("NumericOverflow")
        final List<String> invalidConnectionTimeouts = ImmutableList.of(
                "connectionTimeout=-1;", "connectionTimeout=blah;", "connectionTimeout=" + (long)(Integer.MAX_VALUE + 1));
        for (final String language : languages) {
            for (final String property : properties) {
                final String url = createValidUrl(language, property, true);
                for (final String validConnectionTimeout : validConnectionTimeouts) {
                    final String validUrl = appendProperty(url, validConnectionTimeout, false);
                    Assertions.assertTrue(driver.connect(validUrl, new Properties()) instanceof OpenCypherConnection);
                }
                for (final String invalidConnectionTimeout : invalidConnectionTimeouts) {
                    final String invalidUrl = appendProperty(url, invalidConnectionTimeout, false);
                    Assertions.assertNull(driver.connect(invalidUrl, new Properties()));
                }
            }
        }
    }

    @Test
    void testDriverManagerGetConnection() throws SQLException {
        for (final String language : languages) {
            for (final String property : properties) {
                for (final Boolean semicolon : semicolons) {
                    final String url = createValidUrl(language, property, semicolon);
                    Assertions.assertTrue(DriverManager.getConnection(url) instanceof OpenCypherConnection);
                }
            }
        }
        for (final String url : invalidUrls) {
            Assertions.assertThrows(java.sql.SQLException.class, () -> DriverManager.getConnection(url));
        }
        Assertions.assertThrows(java.sql.SQLException.class, () -> DriverManager.getConnection(null));
    }

    @Test
    void testDriverManagerGetDriver() throws SQLException {
        for (final String language : languages) {
            for (final String property : properties) {
                for (final Boolean semicolon : semicolons) {
                    final String url = createValidUrl(language, property, semicolon);
                    Assertions.assertTrue(DriverManager.getDriver(url) instanceof NeptuneDriver);
                }
            }
        }
        for (final String url : invalidUrls) {
            Assertions.assertThrows(java.sql.SQLException.class, () -> DriverManager.getDriver(url));
        }
        Assertions.assertThrows(java.sql.SQLException.class, () -> DriverManager.getDriver(null));
    }

    // TODO: Look into Driver/NeptuneDriver property string handling.
}
