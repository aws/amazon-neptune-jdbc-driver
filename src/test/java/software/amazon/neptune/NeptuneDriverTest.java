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
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.neptune.opencypher.OpenCypherConnection;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static software.amazon.jdbc.utilities.ConnectionProperty.LOG_LEVEL;

/**
 * Test for NeptuneDriver Object.
 */
public class NeptuneDriverTest {
    private final List<String> invalidUrls = ImmutableList.of(
            "jbdc:neptune:opencyher://;", "jdbc:netune:opencyher://;", "jdbc:neptune:openyher://;",
            "jdbc:neptune:opencyher//;", "jdbc:neptune:opencypher:/");
    private final List<String> languages = ImmutableList.of("opencypher");
    private final List<String> endpoints =
            ImmutableList.of("localhost", "https://foo.bar", "http://foo.bar", "foo.bar", "192.104.204.214");
    private final List<Boolean> semicolons = ImmutableList.of(true, false);
    private final List<String> properties = ImmutableList.of("user=username", "user=username;password=password");
    private java.sql.Driver driver;

    private String createValidUrl(final String language, final String endpoint, final String properties,
                          final boolean trailingSemicolon) {
        String url = String.format("jdbc:neptune:%s://%s", language, endpoint);
        if (!properties.isEmpty()) {
            url += String.format(";%s", properties);
        }
        if (trailingSemicolon) {
            url += ";";
        }
        return url;
    }

    private String appendProperty(final String url, final String property, final boolean trailingSemicolon) {
        String returnUrl = url;
        if (!property.isEmpty()) {
            returnUrl += String.format(";%s", property);
        }
        if (trailingSemicolon) {
            returnUrl += ";";
        }
        return returnUrl;
    }

    @BeforeEach
    void initialize() {
        driver = new NeptuneDriver();
    }

    @Test
    void testAcceptsUrl() throws SQLException {
        for (final String language : languages) {
            for (final String endpoint : endpoints) {
                for (final String property : properties) {
                    for (final Boolean semicolon : semicolons) {
                        final String url = createValidUrl(language, endpoint, property, semicolon);
                    }
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
            for (final String endpoint : endpoints) {
                for (final String property : properties) {
                    for (final Boolean semicolon : semicolons) {
                        final String url = createValidUrl(language, endpoint, property, semicolon);
                        Assertions.assertTrue(driver.connect(url, new Properties()) instanceof OpenCypherConnection);
                    }
                }
            }
        }
        for (final String url : invalidUrls) {
            Assertions.assertNull(driver.connect(url, new Properties()));
        }
    }

    @Test
    void testLogLevelSetting() throws SQLException {
        Assertions.assertEquals(LOG_LEVEL.getDefaultValue(), LogManager.getRootLogger().getLevel());

        final List<String> validLogLevels = ImmutableList.of(
                "", "logLevel=FATAL;", "LogLevel= error", "LOGleVel = InFo ;", "LOGLEVEL=dEbug", "logLEVEL=TRACE;");
        final List<String> invalidLogLevels = ImmutableList.of(
                "logLevel=something;", "LogLevel=5;", "logLevel=;");
        for (final String language : languages) {
            for (final String endpoint : endpoints) {
                for (final String property : properties) {
                    final String url = createValidUrl(language, endpoint, property, false);
                    for (final String logLevel : validLogLevels) {
                        final String validUrl = appendProperty(url, logLevel, false);
                        Assertions.assertTrue(driver.connect(validUrl, new Properties()) instanceof OpenCypherConnection);
                    }
                    for (final String invalidLogLevel : invalidLogLevels) {
                        final String invalidUrl = appendProperty(url, invalidLogLevel, false);
                        // TODO: AN-402 Handle invalid connection string properties
                        //Assertions.assertNull(driver.connect(invalidUrl, new Properties()) instanceof OpenCypherConnection);
                        Assertions.assertTrue(driver.connect(invalidUrl, new Properties()) instanceof OpenCypherConnection);
                    }
                }
            }
        }
        // Reset logging so that it doesn't affect other tests.
        LogManager.getRootLogger().setLevel((Level)LOG_LEVEL.getDefaultValue());
    }

    // TODO: Look into Driver/NeptuneDriver property string handling.
}
