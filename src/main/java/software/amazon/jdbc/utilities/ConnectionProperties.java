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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Class that manages connection properties.
 */
public class ConnectionProperties {
    public static final String APPLICATION_NAME_KEY = "ApplicationName";
    public static final String AWS_CREDENTIALS_PROVIDER_CLASS_KEY = "AwsCredentialsProviderClass";
    public static final String CUSTOM_CREDENTIALS_FILE_PATH_KEY = "CustomCredentialsFilePath";
    public static final String ENDPOINT_KEY = "Endpoint";
    public static final String USER_KEY = "User";
    public static final String PASSWORD_KEY = "Password";
    public static final String ACCESS_KEY_ID_KEY = "AccessKeyId";
    public static final String SECRET_ACCESS_KEY = "SecretAccessKey";
    public static final String SESSION_TOKEN_KEY = "SessionToken";
    public static final String LOG_LEVEL_KEY = "LogLevel";
    public static final String CONNECTION_TIMEOUT_KEY = "ConnectionTimeout";
    public static final String CONNECTION_RETRY_COUNT_KEY = "ConnectionRetryCount";

    public static final Level DEFAULT_LOG_LEVEL = Level.INFO;
    public static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    public static final int DEFAULT_CONNECTION_RETRY_COUNT = 3;

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProperties.class);

    /**
     * Property converter interface.
     * @param <T> Type to convert string property to.
     */
    interface PropertyConverter <T> {
        T convert(String key, String value) throws SQLException;
    }

    private static final Map<String, PropertyConverter<?>> PROPERTIES_MAP = new HashMap<>();
    static {
        PROPERTIES_MAP.put(APPLICATION_NAME_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(AWS_CREDENTIALS_PROVIDER_CLASS_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(CUSTOM_CREDENTIALS_FILE_PATH_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(ENDPOINT_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(ACCESS_KEY_ID_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(USER_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(PASSWORD_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(SECRET_ACCESS_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(SESSION_TOKEN_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(LOG_LEVEL_KEY, (key, value) -> {
            final Map<String, Level> logLevelsMap = ImmutableMap.<String, Level>builder()
                    .put("FATAL", Level.FATAL)
                    .put("ERROR", Level.ERROR)
                    .put("WARN", Level.WARN)
                    .put("INFO", Level.INFO)
                    .put("DEBUG", Level.DEBUG)
                    .put("TRACE", Level.TRACE)
                    .build();
            if (!logLevelsMap.containsKey(value.toUpperCase()) ) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_EXCEPTION,
                        SqlError.INVALID_CONNECTION_PROPERTY, key, value);
            }
            return logLevelsMap.get(value.toUpperCase());
        });
        PROPERTIES_MAP.put(CONNECTION_TIMEOUT_KEY, (key, value) ->  {
            try {
                return Integer.parseUnsignedInt(value);
            } catch (NumberFormatException e) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_EXCEPTION,
                        SqlError.INVALID_CONNECTION_PROPERTY, key, value);
            }
        });
        PROPERTIES_MAP.put(CONNECTION_RETRY_COUNT_KEY, (key, value) ->  {
            try {
                return Integer.parseUnsignedInt(value);
            } catch (NumberFormatException e) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_EXCEPTION,
                        SqlError.INVALID_CONNECTION_PROPERTY, key, value);
            }
        });
    }

    private static final Map<String, Object> DEFAULT_PROPERTIES_MAP = new HashMap<>();
    static {
        DEFAULT_PROPERTIES_MAP.put(ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(LOG_LEVEL_KEY, DEFAULT_LOG_LEVEL);
        DEFAULT_PROPERTIES_MAP.put(CONNECTION_TIMEOUT_KEY, DEFAULT_CONNECTION_TIMEOUT);
        DEFAULT_PROPERTIES_MAP.put(CONNECTION_RETRY_COUNT_KEY, DEFAULT_CONNECTION_RETRY_COUNT);
    }

    private final Properties connectionProperties;

    /**
     * ConnectionProperties constructor.
     * @param properties initial set of connection properties coming from the connection string.
     */
    public ConnectionProperties(@NonNull final Properties properties) throws SQLException {
        this.connectionProperties = resolveProperties(properties);
    }

    /**
     * Resolves input properties and converts them into the valid set of properties.
     * @param inputProperties map of properties coming from the connection string.
     * @return A map of resolved properties.
     * @throws SQLException if invalid input property is detected.
     */
    private Properties resolveProperties(final Properties inputProperties) throws SQLException {
        final Properties outputProperties = new Properties();

        // List of input properties keys used to keep track of unresolved properties.
        final List<Object> inputPropertiesKeys = new ArrayList<>(inputProperties.keySet());

        for (String mapKey : PROPERTIES_MAP.keySet()) {
            for (Map.Entry<Object, Object> entry : inputProperties.entrySet()) {
                final String key = entry.getKey().toString();
                final String value = entry.getValue().toString();
                // Find matching property by comparing keys (case-insensitive)
                if (key.toUpperCase().equals(mapKey.toUpperCase())) {
                    // Insert resolved property into the map.
                    outputProperties.put(mapKey, PROPERTIES_MAP.get(mapKey).convert(key, value));
                    // Remove key for the resolved property.
                    inputPropertiesKeys.remove(key);
                    break;
                }
            }
            // If property was not resolved, insert default value, if it exists.
            if (!outputProperties.containsKey(mapKey)
                    && DEFAULT_PROPERTIES_MAP.containsKey(mapKey)) {
                outputProperties.put(mapKey, DEFAULT_PROPERTIES_MAP.get(mapKey));
            }
        }

        // If there are any unresolved properties left, raise an error.
        if (!inputPropertiesKeys.isEmpty()) {
            // Take the first invalid property to display in the error message.
            final Object key = inputPropertiesKeys.get(0);
            final Object value = inputProperties.get(key);
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    SqlError.INVALID_CONNECTION_PROPERTY, key, value);
        }

        return outputProperties;
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
     * Retrieves Endpoint connection property value.
     * @return Endpoint connection property value.
     */
    public String getEndpoint() {
        return (String)connectionProperties.get(ENDPOINT_KEY);
    }
    /**
     * Retrieves LogLevel connection property value.
     * @return LogLevel connection property value.
     */
    public Level getLogLevel() {
        return (Level)connectionProperties.get(LOG_LEVEL_KEY);
    }
    /**
     * Retrieves ConnectionTimeout connection property value.
     * @return ConnectionTimeout connection property value.
     */
    public int getConnectionTimeout() {
        return (int) connectionProperties.get(CONNECTION_TIMEOUT_KEY);
    }
    /**
     * Retrieves ConnectionRetryCount connection property value.
     * @return ConnectionRetryCount connection property value.
     */
    public int getConnectionRetryCount() {
        return (int) connectionProperties.get(CONNECTION_RETRY_COUNT_KEY);
    }

    /**
     * Check if the property is supported by the driver.
     *
     * @param name The name of the property.
     * @return {@code true} if property is supported; {@code false} otherwise.
     */
    public static boolean isSupportedProperty(final String name) {
        return PROPERTIES_MAP.containsKey(name);
    }
}
