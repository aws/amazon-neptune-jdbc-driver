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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.neptune.opencypher.OpenCypherConnection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

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

    String createValidUrl(final String language, final String endpoint, final String properties,
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

    // TODO: Look into NeptuneDriver property string handling.
}
