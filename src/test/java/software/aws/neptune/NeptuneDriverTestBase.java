/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.aws.neptune.jdbc.Driver;
import software.aws.neptune.jdbc.helpers.HelperFunctions;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.opencypher.OpenCypherConnection;
import software.aws.neptune.opencypher.mock.MockOpenCypherDatabase;

import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public abstract class NeptuneDriverTestBase {
    private static MockOpenCypherDatabase database;
    private static String validEndpoint;
    private static final String PROPERTIES_PATH = "/project.properties";
    private static final String MAJOR_VERSION_KEY = "driver.major.version";
    private static final String MINOR_VERSION_KEY = "driver.minor.version";
    private static final String FULL_VERSION_KEY = "driver.full.version";
    private final List<String> invalidUrls = ImmutableList.of(
            "jbdc:neptune:opencyher://;", "jdbc:netune:opencyher://;", "jdbc:neptune:openyher://;",
            "jdbc:neptune:opencyher//;", "jdbc:neptune:opencypher:/");
    private final List<String> languages = ImmutableList.of("opencypher");
    private final List<Boolean> semicolons = ImmutableList.of(true, false);
    private java.sql.Driver driver;

    protected static String createValidUrl(final boolean useEncryption,
                                           final String language,
                                           final boolean trailingSemicolon) {
        final AuthScheme authScheme = useEncryption ? AuthScheme.IAMSigV4 : AuthScheme.None;
        String url = String.format("jdbc:neptune:%s://%s;useEncryption=%s;authScheme=%s",
                language, validEndpoint, useEncryption, authScheme.toString());
        if (trailingSemicolon) {
            url += ";";
        }
        return url;
    }

    protected static String appendProperty(final String url, final String property, final boolean trailingSemicolon) {
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
    protected static void initializeDatabase(final boolean useEncryption) {
        database = MockOpenCypherDatabase.builder(
                "localhost", NeptuneDriverTestWithEncryption.class.getName(), useEncryption)
                .build();
        validEndpoint = String.format("bolt://%s:%d", "localhost", database.getPort());
    }

    protected static void shutdownTheDatabase() {
        database.shutdown();
    }

    void initialize() {
        driver = new NeptuneDriver();
    }

    void testAcceptsUrl(final boolean useEncryption) throws SQLException {
        for (final String language : languages) {
            for (final Boolean semicolon : semicolons) {
                final String url = createValidUrl(useEncryption, language, semicolon);
                Assertions.assertTrue(driver.acceptsURL(url));
            }
        }
        for (final String url : invalidUrls) {
            Assertions.assertFalse(driver.acceptsURL(url));
        }
    }

    void testConnect(final boolean useEncryption) throws SQLException {
        for (final String language : languages) {
            for (final Boolean semicolon : semicolons) {
                final String validUrl = createValidUrl(useEncryption, language, semicolon);
                Assertions.assertTrue(driver.connect(validUrl, new Properties()) instanceof OpenCypherConnection);
            }
        }
        final String validUrl = createValidUrl(useEncryption, "opencypher", false);
        Assertions.assertNotNull(driver.connect(validUrl, null));

        for (final String invalidUrl : invalidUrls) {
            Assertions.assertNull(driver.connect(invalidUrl, new Properties()));
        }
        Assertions.assertNull(driver.connect(null, new Properties()));
    }

    void testDriverManagerGetConnection(final boolean useEncryption) throws SQLException {
        for (final String language : languages) {
            for (final Boolean semicolon : semicolons) {
                final String url = createValidUrl(useEncryption, language, semicolon);
                Assertions.assertTrue(DriverManager.getConnection(url) instanceof OpenCypherConnection);
            }
        }
        for (final String url : invalidUrls) {
            Assertions.assertThrows(java.sql.SQLException.class, () -> DriverManager.getConnection(url));
        }
    }

    void testDriverManagerGetDriver(final boolean useEncryption) throws SQLException {
        for (final String language : languages) {
            for (final Boolean semicolon : semicolons) {
                final String url = createValidUrl(useEncryption, language, semicolon);
                Assertions.assertTrue(DriverManager.getDriver(url) instanceof NeptuneDriver);
            }
        }
        for (final String url : invalidUrls) {
            Assertions.assertThrows(java.sql.SQLException.class, () -> DriverManager.getDriver(url));
        }
    }

    @Test
    void testDriverProperties() {
        HelperFunctions.expectFunctionThrows(SqlError.FEATURE_NOT_SUPPORTED, () -> driver.getParentLogger());
    }

    @Test
    void testDriverVersion() {
        try (InputStream input = Driver.class.getResourceAsStream(PROPERTIES_PATH)) {
            final Properties properties = new Properties();
            properties.load(input);
            Assertions.assertEquals(driver.getMajorVersion(), Integer.parseInt(properties.getProperty(MAJOR_VERSION_KEY)));
            Assertions.assertEquals(driver.getMinorVersion(), Integer.parseInt(properties.getProperty(MINOR_VERSION_KEY)));

            // Ensure the version did not default
            Assertions.assertNotEquals(driver.getMajorVersion(), 0);
            Assertions.assertTrue(properties.containsKey(FULL_VERSION_KEY));
        } catch (IOException e) {
            Assertions.fail(e.getMessage());
        }
    }
}
