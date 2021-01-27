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
 */

package software.amazon.jdbc.utilities;

import com.google.common.collect.ImmutableMap;
import org.apache.log4j.Level;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static software.amazon.jdbc.utilities.ConnectionProperty.CONNECTION_RETRY_COUNT;
import static software.amazon.jdbc.utilities.ConnectionProperty.CONNECTION_TIMEOUT;
import static software.amazon.jdbc.utilities.ConnectionProperty.LOG_LEVEL;

/**
 * Utility that handles connection properties coming from the connection string.
 */
public class ConnectionPropertiesUtility {

    /**
     * Gets map containing all valid connection properties and their default values.
     * @return Map containing all valid connection properties and their default values.
     */
    private static ImmutableMap<String, Object> getDefaultProperties() {
        return ImmutableMap.of(
                LOG_LEVEL.getConnectionProperty(), LOG_LEVEL.getDefaultValue(),
                CONNECTION_TIMEOUT.getConnectionProperty(), CONNECTION_TIMEOUT.getDefaultValue(),
                CONNECTION_RETRY_COUNT.getConnectionProperty(), CONNECTION_RETRY_COUNT.getDefaultValue()
        );
    }

    /**
     * Extracts valid connection properties from the set of key/value properties originating from the connection string.
     * @param connectionProperties Set of key/value properties originating from the connection string.
     * @return Valid connection properties and their values extracted from the connection string key/value properties.
     */
    public static Properties extractValidProperties(final Properties connectionProperties) {
        final Properties properties = new Properties();
        properties.putAll(getDefaultProperties());
        for (Map.Entry<Object, Object> entry : connectionProperties.entrySet()) {
            final String key = entry.getKey().toString();
            final String value  = entry.getValue().toString();
            try {
                if (LogLevel.matches(key, value)) {
                    properties.put(LOG_LEVEL.getConnectionProperty(), LogLevel.getValue(value));
                } else if (ConnectionTimeout.matches(key, value)) {
                    properties.put(CONNECTION_TIMEOUT.getConnectionProperty(), ConnectionTimeout.getValue(value));
                } else if (ConnectionTimeout.matches(key, value)) {
                    properties.put(CONNECTION_RETRY_COUNT.getConnectionProperty(), ConnectionRetryCount.getValue(value));
                }
            } catch (ExceptionInInitializerError e) {
                // TODO - for testing purposes only - remove try/catch
                System.out.println(e.getMessage());
            }
        }
        return properties;
    }

    /**
     * Utility class that handles Log Level property.
     */
    private static class LogLevel {
        private static final Level DEFAULT_LEVEL = (Level)LOG_LEVEL.getDefaultValue();
        private static final Pattern KEY_PATTERN = Pattern.compile("logLevel", Pattern.CASE_INSENSITIVE);
        private static final Pattern VALUE_PATTERN = Pattern.compile("FATAL|ERROR|WARNINFO|DEBUG|TRACE", Pattern.CASE_INSENSITIVE);
        private static final Map<Pattern, Level> LOG_LEVEL_MAP = ImmutableMap.<Pattern, Level>builder()
                .put(Pattern.compile("FATAL", Pattern.CASE_INSENSITIVE), Level.FATAL)
                .put(Pattern.compile("ERROR", Pattern.CASE_INSENSITIVE), Level.ERROR)
                .put(Pattern.compile("WARN", Pattern.CASE_INSENSITIVE), Level.WARN)
                .put(Pattern.compile("INFO", Pattern.CASE_INSENSITIVE), Level.INFO)
                .put(Pattern.compile("DEBUG", Pattern.CASE_INSENSITIVE), Level.DEBUG)
                .put(Pattern.compile("TRACE", Pattern.CASE_INSENSITIVE), Level.TRACE)
                .build();

        /**
         * Matches key/value property to check whether it represents Log Level.
         * @param key Property key as a case-insensitive string.
         * @param value Property value as a case-insensitive string.
         * @return True if the key/value property represents Log Level property, otherwise false.
         */
        public static boolean matches(final String key, final String value) {
            final Matcher kayMatcher = KEY_PATTERN.matcher(key);
            final Matcher valueMatcher = VALUE_PATTERN.matcher(value);
            return (kayMatcher.matches() && valueMatcher.matches());
        }

        /**
         * Converts Log Level string value to log4j.Level.
         * @param value Log Level as a case-insensitive string.
         * @return Value as a log4j.Level enum.
         */
        public static Level getValue(final String value) {
            for (Map.Entry<Pattern, Level> entry : LOG_LEVEL_MAP.entrySet()) {
                final Matcher matcher = (entry.getKey()).matcher(value);
                if (matcher.matches()) {
                    return entry.getValue();
                }
            }
            return DEFAULT_LEVEL;
        }
    }

    /**
     * Utility class that handles Connection Timeout property.
     */
    private static class ConnectionTimeout {
        private static final int DEFAULT_VALUE = (int)CONNECTION_TIMEOUT.getDefaultValue();
        private static final Pattern KEY_PATTERN = Pattern.compile("connectionTimeout", Pattern.CASE_INSENSITIVE);

        /**
         * Matches key/value property to check whether it represents Connection Timeout.
         * @param key Property key as a case-insensitive string.
         * @param value Property value as a case-insensitive string.
         * @return True if the key/value property represents Connection Timeout property, otherwise false.
         */
        public static boolean matches(final String key, final String value) {
            final Matcher matcher = KEY_PATTERN.matcher(key);
            return matcher.matches() && IntegerValue.matches(value);
        }

        /**
         * Converts Connection Timeout string value to integer.
         * @param value Connection Timeout value as a string.
         * @return Value as an integer, or default value if there is no match.
         */
        public static int getValue(final String value) {
            return IntegerValue.get(value, DEFAULT_VALUE);
        }
    }

    /**
     * Utility class that handles Connection Retry Count property.
     */
    private static class ConnectionRetryCount {
        private static final int DEFAULT_VALUE = (int)CONNECTION_RETRY_COUNT.getDefaultValue();
        private static final Pattern KEY_PATTERN = Pattern.compile("connectionRetryCount", Pattern.CASE_INSENSITIVE);

        /**
         * Matches key/value property to check whether it represents Connection Retry Count.
         * @param key Property key as a case-insensitive string.
         * @param value Property value as a case-insensitive string.
         * @return True if the key/value property represents Connection Retry Count property, otherwise false.
         */
        public static boolean matches(final String key, final String value) {
            final Matcher matcher = KEY_PATTERN.matcher(key);
            return matcher.matches() && IntegerValue.matches(value);
        }

        /**
         * Converts Connection Retry Count string value to integer.
         * @param value Connection Retry Count value as a string.
         * @return Value as an integer, or default value if there is no match.
         */
        public static int getValue(final String value) {
            return IntegerValue.get(value, DEFAULT_VALUE);
        }
    }

    /**
     * Utility class that converts property coming as a string parameter into numeric value.
     */
    private static class IntegerValue {
        /**
         * Matches string value to check whether it represents an unsigned integer.
         * @param value Value as a string.
         * @return True if the value represents an unsigned integer, otherwise false.
         */
        public static boolean matches(final String value) {
            if (value == null) {
                return false;
            }
            try {
                //noinspection ResultOfMethodCallIgnored
                Integer.parseUnsignedInt(value);
            } catch (NumberFormatException e) {
                return false;
            }
            return true;
        }

        /**
         * Converts string value to integer.
         * @param value Value as a string.
         * @return Value as an integer, or default value if there is no match.
         */
        public static int get(final String value, final int defaultValue) {
            try {
                return Integer.parseUnsignedInt(value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
    }
}
