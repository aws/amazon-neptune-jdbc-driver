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
public class ConnectionProperties extends Properties {
    public static final String APPLICATION_NAME_KEY = "ApplicationName";
    public static final String AWS_CREDENTIALS_PROVIDER_CLASS_KEY = "AwsCredentialsProviderClass";
    public static final String CUSTOM_CREDENTIALS_FILE_PATH_KEY = "CustomCredentialsFilePath";
    public static final String ENDPOINT_KEY = "Endpoint";
    public static final String USER_ID_KEY = "User";
    public static final String PASSWORD_KEY = "Password";
    public static final String ACCESS_KEY_ID_KEY = "AccessKeyId";
    public static final String SECRET_ACCESS_KEY_KEY = "SecretAccessKey";
    public static final String SESSION_TOKEN_KEY = "SessionToken";
    public static final String LOG_LEVEL_KEY = "LogLevel";
    public static final String CONNECTION_TIMEOUT_MILLIS_KEY = "ConnectionTimeout";
    public static final String CONNECTION_RETRY_COUNT_KEY = "ConnectionRetryCount";
    public static final String LOGIN_TIMEOUT_SEC_KEY = "LoginTimeoutSec";

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
        PROPERTIES_MAP.put(USER_ID_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(PASSWORD_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(ACCESS_KEY_ID_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(SECRET_ACCESS_KEY_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(SESSION_TOKEN_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(LOG_LEVEL_KEY, (key, value) -> {
            final Map<String, Level> logLevelsMap = ImmutableMap.<String, Level>builder()
                    .put("OFF", Level.OFF)
                    .put("FATAL", Level.FATAL)
                    .put("ERROR", Level.ERROR)
                    .put("WARN", Level.WARN)
                    .put("INFO", Level.INFO)
                    .put("DEBUG", Level.DEBUG)
                    .put("TRACE", Level.TRACE)
                    .put("ALL", Level.ALL)
                    .build();
            if (!logLevelsMap.containsKey(value.toUpperCase()) ) {
                throw invalidConnectionPropertyError(key, value);
            }
            return logLevelsMap.get(value.toUpperCase());
        });
        PROPERTIES_MAP.put(CONNECTION_TIMEOUT_MILLIS_KEY, (key, value) ->  {
            try {
                return Integer.parseUnsignedInt(value);
            } catch (NumberFormatException e) {
                throw invalidConnectionPropertyError(key, value);
            }
        });
        PROPERTIES_MAP.put(CONNECTION_RETRY_COUNT_KEY, (key, value) ->  {
            try {
                return Integer.parseUnsignedInt(value);
            } catch (NumberFormatException e) {
                throw invalidConnectionPropertyError(key, value);
            }
        });
        PROPERTIES_MAP.put(LOGIN_TIMEOUT_SEC_KEY, (key, value) -> {
            try {
                return Integer.parseUnsignedInt(value);
            } catch (NumberFormatException e) {
                throw invalidConnectionPropertyError(key, value);
            }
        });
    }

    private static final Map<String, Object> DEFAULT_PROPERTIES_MAP = new HashMap<>();
    static {
        DEFAULT_PROPERTIES_MAP.put(ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(LOG_LEVEL_KEY, DEFAULT_LOG_LEVEL);
        DEFAULT_PROPERTIES_MAP.put(CONNECTION_TIMEOUT_MILLIS_KEY, DEFAULT_CONNECTION_TIMEOUT);
        DEFAULT_PROPERTIES_MAP.put(CONNECTION_RETRY_COUNT_KEY, DEFAULT_CONNECTION_RETRY_COUNT);
    }

    /**
     * ConnectionProperties constructor.
     */
    public ConnectionProperties() throws SQLException {
        resolveProperties(new Properties());
    }

    /**
     * ConnectionProperties constructor.
     * @param properties initial set of connection properties coming from the connection string.
     */
    public ConnectionProperties(@NonNull final Properties properties) throws SQLException {
        resolveProperties(properties);
    }

    /**
     * Resolves input properties and converts them into the valid set of properties.
     * @param inputProperties map of properties coming from the connection string.
     * @throws SQLException if invalid input property is detected.
     */
    private void resolveProperties(final Properties inputProperties) throws SQLException {
        // List of input properties keys used to keep track of unresolved properties.
        final List<Object> inputPropertiesKeys = new ArrayList<>(inputProperties.keySet());

        for (String mapKey : PROPERTIES_MAP.keySet()) {
            for (Map.Entry<Object, Object> entry : inputProperties.entrySet()) {
                final String key = entry.getKey().toString();
                final String value = entry.getValue().toString();
                // Find matching property by comparing keys (case-insensitive)
                if (key.toUpperCase().equals(mapKey.toUpperCase())) {
                    // Insert resolved property into the map.
                    put(mapKey, PROPERTIES_MAP.get(mapKey).convert(key, value));
                    // Remove key for the resolved property.
                    inputPropertiesKeys.remove(key);
                    break;
                }
            }
            // If property was not resolved, insert default value, if it exists.
            if (!containsKey(mapKey)
                    && DEFAULT_PROPERTIES_MAP.containsKey(mapKey)) {
                put(mapKey, DEFAULT_PROPERTIES_MAP.get(mapKey));
            }
        }

        // If there are any unresolved properties left, raise an error.
        if (!inputPropertiesKeys.isEmpty()) {
            // Take the first invalid property to display in the error message.
            final Object key = inputPropertiesKeys.get(0);
            final Object value = inputProperties.get(key);
            throw invalidConnectionPropertyError(key, value);
        }
    }

    private static SQLException invalidConnectionPropertyError(final Object key, final Object value) {
        return SqlError.createSQLException(
                LOGGER,
                SqlState.CONNECTION_EXCEPTION,
                SqlError.INVALID_CONNECTION_PROPERTY, key, value);
    }

    /**
     * Sets the application name.
     * @param applicationName The application name.
     * @throws SQLException if value is invalid.
     */
    public void setApplicationName(final String applicationName) throws SQLException {
        setProperty(APPLICATION_NAME_KEY,
                (String)PROPERTIES_MAP.get(APPLICATION_NAME_KEY).convert(APPLICATION_NAME_KEY, applicationName));
    }

    /**
     * Gets the application name.
     * @return The application name.
     */
    public String getApplicationName() {
        return getProperty(APPLICATION_NAME_KEY);
    }

    /**
     * Sets the AWS credentials provider class.
     * @param awsCredentialsProviderClass The AWS credentials provider class.
     * @throws SQLException if value is invalid.
     */
    public void setAwsCredentialsProviderClass(final String awsCredentialsProviderClass) throws SQLException {
        setProperty(AWS_CREDENTIALS_PROVIDER_CLASS_KEY,
                (String)PROPERTIES_MAP.get(AWS_CREDENTIALS_PROVIDER_CLASS_KEY)
                        .convert(AWS_CREDENTIALS_PROVIDER_CLASS_KEY, awsCredentialsProviderClass));
    }

    /**
     * Gets the AWS credentials provider class.
     * @return The AWS credentials provider class.
     */
    public String getAwsCredentialsProviderClass() {
        return getProperty(AWS_CREDENTIALS_PROVIDER_CLASS_KEY);
    }

    /**
     * Sets the custom credentials filepath.
     * @param customCredentialsFilePath The custom credentials filepath.
     * @throws SQLException if value is invalid.
     */
    public void setCustomCredentialsFilePath(final String customCredentialsFilePath) throws SQLException {
        setProperty(CUSTOM_CREDENTIALS_FILE_PATH_KEY,
                (String)PROPERTIES_MAP.get(CUSTOM_CREDENTIALS_FILE_PATH_KEY)
                        .convert(CUSTOM_CREDENTIALS_FILE_PATH_KEY, customCredentialsFilePath));
    }

    /**
     * Gets the custom credentials filepath.
     * @return The custom credentials filepath.
     */
    public String getCustomCredentialsFilePath() {
        return getProperty(CUSTOM_CREDENTIALS_FILE_PATH_KEY);
    }

    /**
     * Sets the connection endpoint.
     * @param endpoint The connection endpoint.
     * @throws SQLException if value is invalid.
     */
    public void setEndpoint(final String endpoint) throws SQLException {
        setProperty(ENDPOINT_KEY,
                (String)PROPERTIES_MAP.get(ENDPOINT_KEY).convert(ENDPOINT_KEY, endpoint));
    }

    /**
     * Gets the connection endpoint.
     * @return The connection endpoint.
     */
    public String getEndpoint() {
        return getProperty(ENDPOINT_KEY);
    }

    /**
     * Sets the user Id.
     * @param userId The user Id.
     * @throws SQLException if value is invalid.
     */
    public void setUserId(final String userId) throws SQLException {
        setProperty(USER_ID_KEY,
                (String)PROPERTIES_MAP.get(USER_ID_KEY).convert(USER_ID_KEY, userId));
    }

    /**
     * Gets the user Id.
     * @return The user Id.
     */
    public String getUserId() {
        return getProperty(USER_ID_KEY);
    }

    /**
     * Sets the password.
     * @param password The password.
     * @throws SQLException if value is invalid.
     */
    public void setPassword(final String password) throws SQLException {
        setProperty(PASSWORD_KEY,
                (String)PROPERTIES_MAP.get(PASSWORD_KEY).convert(PASSWORD_KEY, password));
    }

    /**
     * Gets the password.
     * @return The password.
     */
    public String getPassword() {
        return getProperty(PASSWORD_KEY);
    }


    /**
     * Sets the access key Id.
     * @param accessKeyId The access key Id.
     * @throws SQLException if value is invalid.
     */
    public void setAccessKeyId(final String accessKeyId) throws SQLException {
        setProperty(ACCESS_KEY_ID_KEY,
                (String)PROPERTIES_MAP.get(ACCESS_KEY_ID_KEY).convert(ACCESS_KEY_ID_KEY, accessKeyId));
    }

    /**
     * Gets the access key Id.
     * @return The access key Id.
     */
    public String getAccessKeyId() {
        return getProperty(ACCESS_KEY_ID_KEY);
    }

    /**
     * Sets the secret access key.
     * @param secretAccessKey The secret access key.
     * @throws SQLException if value is invalid.
     */
    public void setSecretAccessKey(final String secretAccessKey) throws SQLException {
        setProperty(SECRET_ACCESS_KEY_KEY,
                (String)PROPERTIES_MAP.get(SECRET_ACCESS_KEY_KEY).convert(SECRET_ACCESS_KEY_KEY, secretAccessKey));
    }

    /**
     * Gets the secret access key.
     * @return The secret access key.
     */
    public String getSecretAccessKey() {
        return getProperty(SECRET_ACCESS_KEY_KEY);
    }

    /**
     * Sets the session token.
     * @param sessionToken The session token.
     * @throws SQLException if value is invalid.
     */
    public void setSessionToken(final String sessionToken) throws SQLException {
        setProperty(SESSION_TOKEN_KEY,
                (String)PROPERTIES_MAP.get(SESSION_TOKEN_KEY).convert(SESSION_TOKEN_KEY, sessionToken));
    }

    /**
     * Gets the session token.
     * @return The session token.
     */
    public String getSessionToken() {
        return getProperty(SESSION_TOKEN_KEY);
    }

    /**
     * Sets the logging level.
     * @param logLevel The logging level.
     * @throws SQLException if value is invalid.
     */
    public void setLogLevel(final Level logLevel) throws SQLException {
        put(LOG_LEVEL_KEY, logLevel);
    }

    /**
     * Gets the logging level.
     * @return The logging level.
     */
    public Level getLogLevel() {
        return (Level)get(LOG_LEVEL_KEY);
    }

    /**
     * Sets the connection timeout in milliseconds.
     * @param timeoutMillis The connection timeout in milliseconds.
     * @throws SQLException if value is invalid.
     */
    public void setConnectionTimeoutMillis(final int timeoutMillis) throws SQLException {
        if (timeoutMillis < 0) {
            throw invalidConnectionPropertyError(CONNECTION_TIMEOUT_MILLIS_KEY, timeoutMillis);
        }
        put(CONNECTION_TIMEOUT_MILLIS_KEY, timeoutMillis);
    }

    /**
     * Gets the connection timeout in milliseconds.
     * @return The connection timeout in milliseconds.
     */
    public int getConnectionTimeoutMillis() {
        return (int)get(CONNECTION_TIMEOUT_MILLIS_KEY);
    }

    /**
     * Sets the connection retry count.
     * @param retryCount The connection retry count.
     * @throws SQLException if value is invalid.
     */
    public void setConnectionRetryCount(final int retryCount) throws SQLException {
        if (retryCount < 0) {
            throw invalidConnectionPropertyError(CONNECTION_RETRY_COUNT_KEY, retryCount);
        }
        put(CONNECTION_RETRY_COUNT_KEY, retryCount);
    }

    /**
     * Gets the connection retry count.
     * @return The connection retry count.
     */
    public int getConnectionRetryCount() {
        return (int) get(CONNECTION_RETRY_COUNT_KEY);
    }

    /**
     * Sets the timeout for opening a connection.
     * @param loginTimeout The connect timeout in seconds.
     * @throws SQLException if value is invalid.
     */
    public void setLoginTimeout(final int loginTimeout) throws SQLException {
        if (loginTimeout < 0) {
            throw invalidConnectionPropertyError(LOGIN_TIMEOUT_SEC_KEY, loginTimeout);
        }
        put(LOGIN_TIMEOUT_SEC_KEY, loginTimeout);
    }

    /**
     * Gets the timeout for opening a connection.
     * @return The connect timeout in seconds.
     */
    public int getLoginTimeout() {
        return (int) get(LOGIN_TIMEOUT_SEC_KEY);
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
