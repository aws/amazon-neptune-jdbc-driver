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
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static software.amazon.jdbc.utilities.ConnectionProperty.CONNECTION_RETRY_COUNT;
import static software.amazon.jdbc.utilities.ConnectionProperty.CONNECTION_TIMEOUT;
import static software.amazon.jdbc.utilities.ConnectionProperty.LOG_LEVEL;

/**
 * Class that manages connection properties.
 */
public class ConnectionProperties {
    private Properties connectionProperties;

    private static final Map<String, PropertyConverter<?>> PROPERTIES_MAP = new HashMap<>();
    static {
        PROPERTIES_MAP.put(LOG_LEVEL.getConnectionProperty(), LOG_LEVEL.getPropertyConverter());
        PROPERTIES_MAP.put(CONNECTION_TIMEOUT.getConnectionProperty(), CONNECTION_TIMEOUT.getPropertyConverter());
        PROPERTIES_MAP.put(CONNECTION_RETRY_COUNT.getConnectionProperty(), CONNECTION_RETRY_COUNT.getPropertyConverter());
    }

    /**
     * ConnectionProperties constructor.
     * @param properties initial set of connection properties coming from the connection string.
     */
    public ConnectionProperties(@NonNull final Properties properties) {
        this.connectionProperties = new Properties();
        for (Map.Entry<String, PropertyConverter<?>> entry : PROPERTIES_MAP.entrySet()) {
            connectionProperties.put(entry.getKey(), entry.getValue().convert(properties));
        }

        // If any invalid properties are left, raise an error
        if (!properties.isEmpty()) {
            // TODO: AN-402 Handle invalid connection string properties
            // For now, just add them as is, but avoid overwriting values if key is a duplicate.
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                if (!connectionProperties.containsKey(entry.getKey())) {
                    connectionProperties.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }


    /**
     * Gets all connection properties.
     * @return all connection properties.
     */
    public Properties getAll() {
        return connectionProperties;
    }

    // TODO: AN-405 - Redo Connection getClientInfo() and setClientInfo()
    // This method should be removed.
    /**
     * Clear content of the connectionProperties map.
     */
    public void clear() {
        connectionProperties.clear();
    }

    /**
     * Puts a Connection property into the map.
     * @param key Connection property key.
     * @param value Connection property value.
     */
    public void put(final String key, final Object value) {
        connectionProperties.put(key, value);
    }

    /**
     * Gets connection property string value for the given key.
     * @return Connection property value for the given key.
     */
    public String get(final String key) {
        return connectionProperties.get(key).toString();
    }

    /**
     * Retrieves LogLevel connection property value.
     * @return LogLevel connection property value.
     */
    public Level getLogLevel() {
        return (Level)connectionProperties.get(LOG_LEVEL.getConnectionProperty());
    }

    /**
     * Retrieves ConnectionTimeout connection property value.
     * @return ConnectionTimeout connection property value.
     */
    public int getConnectionTimeout() {
        return (int) connectionProperties.get(CONNECTION_TIMEOUT.getConnectionProperty());
    }

    /**
     * Retrieves ConnectionRetryCount connection property value.
     * @return ConnectionRetryCount connection property value.
     */
    public int getConnectionRetryCount() {
        return (int) connectionProperties.get(CONNECTION_RETRY_COUNT.getConnectionProperty());
    }

    /**
     * Converter class that converts string property to its proper type.
     * @param <T> Type to convert to.
     */
    public abstract static class PropertyConverter<T> {

        /**
         * Function to perform matching connection string key/value property.
         *
         * @param value String value to convert.
         * @return Converted value.
         */
        abstract boolean matches(String key, String value);

        /**
         * Function to perform conversion.
         *
         * @param value String value to convert to T type.
         * @return Converted value.
         */
        abstract T getValue(String value);

        /**
         * Function that returns default value.
         * @return Default value.
         */
        abstract T getDefaultValue();

        /**
         * Function to find matching property and perform conversion.
         *
         * @param connectionProperties Map containing connection properties came from the connection string.
         * @return Converted value, or default value if no matching property found in connection properties map.
         */
        public T convert(final Properties connectionProperties) {
            for (Map.Entry<Object, Object> entry : connectionProperties.entrySet()) {
                final String key = entry.getKey().toString();
                final String value = entry.getValue().toString();
                if (matches(key, value)) {
                    // remove property that is matched
                    connectionProperties.remove(key);
                    return (T) getValue(value);
                }
            }
            return (T) getDefaultValue();
        }
    }

    /**
     * Utility class that handles LogLevel property.
     */
    public static class LogLevelConverter extends PropertyConverter<Level> {
        private static final Pattern KEY_PATTERN = Pattern.compile("logLevel", Pattern.CASE_INSENSITIVE);
        private static final Pattern VALUE_PATTERN = Pattern.compile("FATAL|ERROR|WARN|INFO|DEBUG|TRACE", Pattern.CASE_INSENSITIVE);
        private static final Map<String, Level> LOG_LEVEL_MAP = ImmutableMap.<String, Level>builder()
                .put("FATAL", Level.FATAL)
                .put("ERROR", Level.ERROR)
                .put("WARN", Level.WARN)
                .put("INFO", Level.INFO)
                .put("DEBUG", Level.DEBUG)
                .put("TRACE", Level.TRACE)
                .build();

        @Override
        public boolean matches(final String key, final String value) {
            return KEY_PATTERN.matcher(key).matches()
                    && VALUE_PATTERN.matcher(value).matches();
        }

        @Override
        public Level getValue(final String value) {
            return LOG_LEVEL_MAP.get(value.toUpperCase());
        }

        @Override
        public Level getDefaultValue() {
            return (Level)LOG_LEVEL.getDefaultValue();
        }
    }

    /**
     * Utility class that handles ConnectionTimeout property.
     */
    public static class ConnectionTimeoutConverter extends PropertyConverter<Integer> {
        private static final Pattern KEY_PATTERN = Pattern.compile("connectionTimeout", Pattern.CASE_INSENSITIVE);

        @Override
        public boolean matches(final String key, final String value) {
            return KEY_PATTERN.matcher(key).matches()
                    && isUnsignedInteger(value);
        }

        @Override
        public Integer getValue(final String value) {
            return toUnsignedInteger(value);
        }

        @Override
        public Integer getDefaultValue() {
            return (Integer)CONNECTION_TIMEOUT.getDefaultValue();
        }
    }

    /**
     * Utility class that handles ConnectionRetryCount property.
     */
    public static class ConnectionRetryCountConverter extends PropertyConverter<Integer> {
        private static final Pattern KEY_PATTERN = Pattern.compile("connectionRetryCount", Pattern.CASE_INSENSITIVE);

        @Override
        public boolean matches(final String key, final String value) {
            return KEY_PATTERN.matcher(key).matches()
                    && isUnsignedInteger(value);
        }

        @Override
        public Integer getValue(final String value) {
            return toUnsignedInteger(value);
        }

        @Override
        public Integer getDefaultValue() {
            return (Integer)CONNECTION_RETRY_COUNT.getDefaultValue();
        }
    }

    /**
     * Matches string value to check whether it represents an unsigned integer.
     * @param value Value as a string.
     * @return True if the value represents an unsigned integer, otherwise false.
     */
    private static boolean isUnsignedInteger(final String value) {
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
    private static Integer toUnsignedInteger(final String value) {
        return Integer.parseUnsignedInt(value);
    }
}
