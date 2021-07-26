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

package software.aws.neptune;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import software.aws.jdbc.utilities.AuthScheme;
import software.aws.jdbc.utilities.ConnectionProperties;
import software.aws.neptune.opencypher.OpenCypherConnectionProperties;
import java.util.List;
import java.util.Properties;

public abstract class ConnectionPropertiesTestBase {
    protected static final boolean DEFAULT_FALSE = false;
    protected static final String DEFAULT_EMPTY_STRING = "";
    protected static final int NO_DEFAULT_INT = 0;
    protected static final boolean NO_DEFAULT_BOOL = false;
    protected static final String NO_DEFAULT_STRING = null;

    protected abstract void assertDoesNotThrowOnNewConnectionProperties(final Properties properties);

    protected abstract void assertThrowsOnNewConnectionProperties(final Properties properties);

    protected abstract <T> void assertPropertyValueEqualsToExpected(final String key, final T expectedValue);

    protected void testAuthSchemeViaConstructor() {

        final List<String> emptyAuthSchemes = ImmutableList.of(
                "", " ");
        final List<String> validAuthSchemes = ImmutableList.of(
                "NONE", "none", "IAMSigV4", "iamSIGV4", "IAMRole", "IaMRoLe");
        final List<String> invalidAuthSchemes = ImmutableList.of(
                "-1;", "100;", "46hj7;", "foo;");

        // Verify empty string is converted to a default value.
        for (final String emptyValue : emptyAuthSchemes) {
            final Properties properties = new Properties();
            properties.put(ConnectionProperties.AUTH_SCHEME_KEY, emptyValue);
            assertDoesNotThrowOnNewConnectionProperties(properties);
            assertPropertyValueEqualsToExpected(
                    ConnectionProperties.AUTH_SCHEME_KEY, ConnectionProperties.DEFAULT_AUTH_SCHEME);
        }

        // Verify valid property value is set.
        for (final String validValue : validAuthSchemes) {
            // Convert string to enum.
            Assertions.assertNotNull(
                    AuthScheme.fromString(validValue)
            );
            final Properties properties = new Properties();
            properties.put(ConnectionProperties.AUTH_SCHEME_KEY, validValue);
            assertDoesNotThrowOnNewConnectionProperties(properties);
            assertPropertyValueEqualsToExpected(
                    ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.fromString(validValue));
        }

        // Verify invalid property value throws error.
        for (final String invalidValue : invalidAuthSchemes) {
            // Test failure to convert invalid string to enum.
            Assertions.assertNull(
                    AuthScheme.fromString(invalidValue)
            );
            final Properties properties = new Properties();
            properties.setProperty(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, invalidValue);
            assertThrowsOnNewConnectionProperties(properties);
        }
    }

    protected void testLogLevelSettingViaConstructor() {

        final List<String> validLogLevels = ImmutableList.of(
                "", "Off", "FATAL", "error", "InFo", "dEbug", "TRACE", "All");
        final List<String> invalidLogLevels = ImmutableList.of(
                "something", "5");

        // Verify valid property value doesn't throw error.
        for (final String validValue : validLogLevels) {
            // Set property through constructor.
            final Properties properties = new Properties();
            properties.put(ConnectionProperties.LOG_LEVEL_KEY, validValue);
            assertDoesNotThrowOnNewConnectionProperties(properties);
        }

        // Verify invalid property value throws error.
        for (final String invalidValue : invalidLogLevels) {
            final Properties properties = new Properties();
            properties.setProperty(ConnectionProperties.LOG_LEVEL_KEY, invalidValue);
            assertThrowsOnNewConnectionProperties(properties);
        }
    }

    protected void testStringPropertyViaConstructor(
            final String key) {
        testStringPropertyViaConstructor(
                new Properties(), key, NO_DEFAULT_STRING, false);
    }

    protected void testStringPropertyViaConstructor(
            final String key,
            final String defaultValue) {
        testStringPropertyViaConstructor(
                new Properties(), key, defaultValue);
    }

    protected void testStringPropertyViaConstructor(
            final Properties initProperties,
            final String key,
            final String defaultValue) {
        testStringPropertyViaConstructor(
                initProperties, key, defaultValue, true);
    }

    private void testStringPropertyViaConstructor(
            final Properties initProperties,
            final String key,
            final String defaultValue,
            final boolean hasDefault) {

        final List<String> testValues = ImmutableList.of("foo", "bar");

        if (hasDefault) {
            final Properties properties = new Properties();
            properties.putAll(initProperties);
            assertDoesNotThrowOnNewConnectionProperties(properties);
            assertPropertyValueEqualsToExpected(key, defaultValue);
        }

        // Verify valid property value doesn't throw error.
        for (final String value : testValues) {
            final Properties properties = new Properties();
            properties.putAll(initProperties);
            properties.put(key, value);
            assertDoesNotThrowOnNewConnectionProperties(properties);
            assertPropertyValueEqualsToExpected(key, value);
        }
    }

    protected void testIntegerPropertyViaConstructor(
            final String key) {
        testIntegerPropertyViaConstructor(
                new Properties(), key, NO_DEFAULT_INT, false);
    }

    protected void testIntegerPropertyViaConstructor(
            final String key,
            final int defaultValue) {
        testIntegerPropertyViaConstructor(
                new Properties(), key, defaultValue);
    }

    protected void testIntegerPropertyViaConstructor(
            final Properties initProperties,
            final String key,
            final int defaultValue) {
        testIntegerPropertyViaConstructor(
                initProperties, key, defaultValue, true);
    }

    private void testIntegerPropertyViaConstructor(
            final Properties initProperties,
            final String key,
            final int defaultValue,
            final boolean hasDefault) {

        final List<String> validValues = ImmutableList.of(
                "0", "5", "10000");
        final List<String> invalidValues = ImmutableList.of(
                "-1", "blah", String.valueOf((long) Integer.MAX_VALUE + 1000));

        if (hasDefault) {
            final Properties properties = new Properties();
            properties.putAll(initProperties);
            assertDoesNotThrowOnNewConnectionProperties(properties);
            assertPropertyValueEqualsToExpected(key, defaultValue);
        }

        // Verify valid property value doesn't throw error.
        for (final String validValue : validValues) {
            final Properties properties = new Properties();
            properties.putAll(initProperties);
            properties.put(key, validValue);
            assertDoesNotThrowOnNewConnectionProperties(properties);
            assertPropertyValueEqualsToExpected(key, Integer.parseInt(validValue));
        }

        // Verify invalid property value throws error.
        for (final String invalidValue : invalidValues) {
            final Properties properties = new Properties();
            properties.putAll(initProperties);
            properties.setProperty(key, invalidValue);
            assertThrowsOnNewConnectionProperties(properties);
        }
    }

    protected void testBooleanPropertyViaConstructor(
            final String key) {
        testBooleanPropertyViaConstructor(
                new Properties(), key, NO_DEFAULT_BOOL, false);
    }

    protected void testBooleanPropertyViaConstructor(
            final String key,
            final boolean defaultValue) {
        testBooleanPropertyViaConstructor(
                new Properties(), key, defaultValue);
    }

    protected void testBooleanPropertyViaConstructor(
            final Properties initProperties,
            final String key,
            final boolean defaultValue) {
        testBooleanPropertyViaConstructor(
                initProperties, key, defaultValue, true);
    }

    private void testBooleanPropertyViaConstructor(
            final Properties initProperties,
            final String key,
            final boolean defaultValue,
            final boolean hasDefault) {

        final List<String> validTrueValues = ImmutableList.of(
                "1", "true", "TRUE", "tRue");
        final List<String> validFalseValues = ImmutableList.of(
                "0", "false", "FALSE", "FaLSe");
        final List<String> invalidValues = ImmutableList.of(
                "-1;", "100;", "46hj7;", "foo;");

        if (hasDefault) {
            final Properties properties = new Properties();
            properties.putAll(initProperties);
            assertDoesNotThrowOnNewConnectionProperties(properties);
            assertPropertyValueEqualsToExpected(key, defaultValue);
        }

        // Verify valid TRUE property value is set.
        for (final String validTrueValue : validTrueValues) {
            final Properties properties = new Properties();
            properties.putAll(initProperties);
            properties.put(key, validTrueValue);
            assertDoesNotThrowOnNewConnectionProperties(properties);
            assertPropertyValueEqualsToExpected(key, true);
        }

        // Verify valid FALSE property value is set.
        for (final String validFalseValue : validFalseValues) {
            final Properties properties = new Properties();
            properties.putAll(initProperties);
            properties.put(key, validFalseValue);
            assertDoesNotThrowOnNewConnectionProperties(properties);
            assertPropertyValueEqualsToExpected(key, false);
        }

        // Verify invalid property value throws error.
        for (final String invalidValue : invalidValues) {
            final Properties properties = new Properties();
            properties.putAll(initProperties);
            properties.setProperty(key, invalidValue);
            assertThrowsOnNewConnectionProperties(properties);
        }
    }
}

