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
import java.util.regex.Pattern;

/**
 * Class that manages connection properties.
 */
public class ConnectionProperties extends Properties {
    public static final String APPLICATION_NAME_KEY = "ApplicationName";
    public static final String ENDPOINT_KEY = "Endpoint";
    public static final String LOG_LEVEL_KEY = "LogLevel";
    public static final String CONNECTION_TIMEOUT_MILLIS_KEY = "ConnectionTimeout";
    public static final String CONNECTION_RETRY_COUNT_KEY = "ConnectionRetryCount";
    public static final String AUTH_SCHEME_KEY = "AuthScheme";
    public static final String USE_ENCRYPTION_KEY = "UseEncryption";
    public static final String REGION_KEY = "Region";

    // TODO: Revisit. We should probably support these.
    public static final String AWS_CREDENTIALS_PROVIDER_CLASS_KEY = "AwsCredentialsProviderClass";
    public static final String CUSTOM_CREDENTIALS_FILE_PATH_KEY = "CustomCredentialsFilePath";

    public static final Level DEFAULT_LOG_LEVEL = Level.INFO;
    public static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 5000;
    public static final int DEFAULT_CONNECTION_RETRY_COUNT = 3;
    public static final int DEFAULT_LOGIN_TIMEOUT_SEC = 0;
    public static final AuthScheme DEFAULT_AUTH_SCHEME = AuthScheme.None;
    public static final boolean DEFAULT_USE_ENCRYPTION = true;
    public static final Map<String, Object> DEFAULT_PROPERTIES_MAP = new HashMap<>();
    private static final Map<String, PropertyConverter<?>> PROPERTIES_MAP = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProperties.class);

    static {
        PROPERTIES_MAP.put(APPLICATION_NAME_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(AWS_CREDENTIALS_PROVIDER_CLASS_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(CUSTOM_CREDENTIALS_FILE_PATH_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(ENDPOINT_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(REGION_KEY, (key, value) -> value);
        PROPERTIES_MAP.put(LOG_LEVEL_KEY, (key, value) -> {
            if (isWhitespace(value)) {
                return DEFAULT_LOG_LEVEL;
            }
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
            if (!logLevelsMap.containsKey(value.toUpperCase())) {
                throw invalidConnectionPropertyError(key, value);
            }
            return logLevelsMap.get(value.toUpperCase());
        });
        PROPERTIES_MAP.put(CONNECTION_TIMEOUT_MILLIS_KEY, (key, value) -> {
            if (isWhitespace(value)) {
                return DEFAULT_CONNECTION_TIMEOUT_MILLIS;
            }
            try {
                final int intValue = Integer.parseUnsignedInt(value);
                if (intValue < 0) {
                    throw invalidConnectionPropertyError(key, value);
                }
                return intValue;
            } catch (final NumberFormatException e) {
                throw invalidConnectionPropertyError(key, value);
            }
        });
        PROPERTIES_MAP.put(CONNECTION_RETRY_COUNT_KEY, (key, value) -> {
            if (isWhitespace(value)) {
                return DEFAULT_CONNECTION_RETRY_COUNT;
            }
            try {
                final int intValue = Integer.parseUnsignedInt(value);
                if (intValue < 0) {
                    throw invalidConnectionPropertyError(key, value);
                }
                return intValue;
            } catch (final NumberFormatException e) {
                throw invalidConnectionPropertyError(key, value);
            }
        });
        PROPERTIES_MAP.put(AUTH_SCHEME_KEY, (key, value) -> {
            if (isWhitespace(value)) {
                return DEFAULT_AUTH_SCHEME;
            }
            if (AuthScheme.fromString(value) == null) {
                throw invalidConnectionPropertyError(key, value);
            }
            return AuthScheme.fromString(value);
        });
        PROPERTIES_MAP.put(USE_ENCRYPTION_KEY, (key, value) -> {
            if (isWhitespace(value)) {
                return DEFAULT_USE_ENCRYPTION;
            }
            final Map<String, Boolean> stringBooleanMap = ImmutableMap.of(
                    "1", true, "true", true,
                    "0", false, "false", false);
            if (!stringBooleanMap.containsKey(value.toLowerCase())) {
                throw invalidConnectionPropertyError(key, value);
            }
            return stringBooleanMap.get(value.toLowerCase());
        });
    }

    static {
        DEFAULT_PROPERTIES_MAP.put(ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(LOG_LEVEL_KEY, DEFAULT_LOG_LEVEL);
        DEFAULT_PROPERTIES_MAP.put(CONNECTION_TIMEOUT_MILLIS_KEY, DEFAULT_CONNECTION_TIMEOUT_MILLIS);
        DEFAULT_PROPERTIES_MAP.put(CONNECTION_RETRY_COUNT_KEY, DEFAULT_CONNECTION_RETRY_COUNT);
        DEFAULT_PROPERTIES_MAP.put(AUTH_SCHEME_KEY, DEFAULT_AUTH_SCHEME);
        DEFAULT_PROPERTIES_MAP.put(USE_ENCRYPTION_KEY, DEFAULT_USE_ENCRYPTION);
        DEFAULT_PROPERTIES_MAP.put(REGION_KEY, "");
    }

    /**
     * ConnectionProperties constructor.
     */
    public ConnectionProperties() throws SQLException {
        resolveProperties(new Properties());
    }

    /**
     * ConnectionProperties constructor.
     *
     * @param properties initial set of connection properties coming from the connection string.
     */
    public ConnectionProperties(@NonNull final Properties properties) throws SQLException {
        resolveProperties(properties);
    }

    private static boolean isWhitespace(@NonNull final String value) {
        return Pattern.matches("^\\s*$", value);
    }

    private static SQLException invalidConnectionPropertyError(final Object key, final Object value) {
        return SqlError.createSQLException(
                LOGGER,
                SqlState.CONNECTION_EXCEPTION,
                SqlError.INVALID_CONNECTION_PROPERTY, key, value);
    }

    private static SQLException missingConnectionPropertyError(final String reason) {
        return SqlError.createSQLException(
                LOGGER,
                SqlState.CONNECTION_EXCEPTION,
                SqlError.MISSING_CONNECTION_PROPERTY, reason);
    }

    private static SQLException invalidConnectionPropertyValueError(final String key, final String reason) {
        return SqlError.createSQLException(
                LOGGER,
                SqlState.CONNECTION_EXCEPTION,
                SqlError.INVALID_VALUE_CONNECTION_PROPERTY, key, reason);
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

    /**
     * Resolves input properties and converts them into the valid set of properties.
     *
     * @param inputProperties map of properties coming from the connection string.
     * @throws SQLException if invalid input property is detected.
     */
    private void resolveProperties(final Properties inputProperties) throws SQLException {
        // List of input properties keys used to keep track of unresolved properties.
        final List<Object> inputPropertiesKeys = new ArrayList<>(inputProperties.keySet());

        for (final String mapKey : PROPERTIES_MAP.keySet()) {
            for (final Map.Entry<Object, Object> entry : inputProperties.entrySet()) {
                final String key = entry.getKey().toString();
                final String value = entry.getValue().toString();
                // Find matching property by comparing keys (case-insensitive)
                if (key.equalsIgnoreCase(mapKey)) {
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

        // If IAMSigV4 is specified, we need the region provided to us.
        if (getAuthScheme().equals(AuthScheme.IAMSigV4)) {
            if (getRegion().isEmpty()) {
                throw missingConnectionPropertyError("A Region must be provided to use IAMSigV4 Authentication");
            }
            if (!getUseEncryption()) {
                throw invalidConnectionPropertyValueError(USE_ENCRYPTION_KEY, "Encryption must be enabled if IAMSigV4 is used.");
            }
        }
    }

    /**
     * Gets the application name.
     *
     * @return The application name.
     */
    public String getApplicationName() {
        return getProperty(APPLICATION_NAME_KEY);
    }

    /**
     * Sets the application name.
     *
     * @param applicationName The application name.
     * @throws SQLException if value is invalid.
     */
    public void setApplicationName(final String applicationName) throws SQLException {
        setProperty(APPLICATION_NAME_KEY,
                (String) PROPERTIES_MAP.get(APPLICATION_NAME_KEY).convert(APPLICATION_NAME_KEY, applicationName));
    }

    /**
     * Gets the AWS credentials provider class.
     *
     * @return The AWS credentials provider class.
     */
    public String getAwsCredentialsProviderClass() {
        return getProperty(AWS_CREDENTIALS_PROVIDER_CLASS_KEY);
    }

    /**
     * Sets the AWS credentials provider class.
     *
     * @param awsCredentialsProviderClass The AWS credentials provider class.
     * @throws SQLException if value is invalid.
     */
    public void setAwsCredentialsProviderClass(final String awsCredentialsProviderClass) throws SQLException {
        setProperty(AWS_CREDENTIALS_PROVIDER_CLASS_KEY,
                (String) PROPERTIES_MAP.get(AWS_CREDENTIALS_PROVIDER_CLASS_KEY)
                        .convert(AWS_CREDENTIALS_PROVIDER_CLASS_KEY, awsCredentialsProviderClass));
    }

    /**
     * Gets the custom credentials filepath.
     *
     * @return The custom credentials filepath.
     */
    public String getCustomCredentialsFilePath() {
        return getProperty(CUSTOM_CREDENTIALS_FILE_PATH_KEY);
    }

    /**
     * Sets the custom credentials filepath.
     *
     * @param customCredentialsFilePath The custom credentials filepath.
     * @throws SQLException if value is invalid.
     */
    public void setCustomCredentialsFilePath(final String customCredentialsFilePath) throws SQLException {
        setProperty(CUSTOM_CREDENTIALS_FILE_PATH_KEY,
                (String) PROPERTIES_MAP.get(CUSTOM_CREDENTIALS_FILE_PATH_KEY)
                        .convert(CUSTOM_CREDENTIALS_FILE_PATH_KEY, customCredentialsFilePath));
    }

    /**
     * Gets the connection endpoint.
     *
     * @return The connection endpoint.
     */
    public String getEndpoint() {
        return getProperty(ENDPOINT_KEY);
    }

    /**
     * Sets the connection endpoint.
     *
     * @param endpoint The connection endpoint.
     * @throws SQLException if value is invalid.
     */
    public void setEndpoint(final String endpoint) throws SQLException {
        setProperty(ENDPOINT_KEY,
                (String) PROPERTIES_MAP.get(ENDPOINT_KEY).convert(ENDPOINT_KEY, endpoint));
    }

    /**
     * Gets the logging level.
     *
     * @return The logging level.
     */
    public Level getLogLevel() {
        return (Level) get(LOG_LEVEL_KEY);
    }

    /**
     * Sets the logging level.
     *
     * @param logLevel The logging level.
     * @throws SQLException if value is invalid.
     */
    public void setLogLevel(final Level logLevel) throws SQLException {
        if (logLevel == null) {
            throw invalidConnectionPropertyError(LOG_LEVEL_KEY, logLevel);
        }
        put(LOG_LEVEL_KEY, logLevel);
    }

    /**
     * Gets the connection timeout in milliseconds.
     *
     * @return The connection timeout in milliseconds.
     */
    public int getConnectionTimeoutMillis() {
        return (int) get(CONNECTION_TIMEOUT_MILLIS_KEY);
    }

    /**
     * Sets the connection timeout in milliseconds.
     *
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
     * Gets the connection retry count.
     *
     * @return The connection retry count.
     */
    public int getConnectionRetryCount() {
        return (int) get(CONNECTION_RETRY_COUNT_KEY);
    }

    /**
     * Sets the connection retry count.
     *
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
     * Gets the authentication scheme.
     *
     * @return The authentication scheme.
     */
    public AuthScheme getAuthScheme() {
        return (AuthScheme) get(AUTH_SCHEME_KEY);
    }

    /**
     * Sets the authentication scheme.
     *
     * @param authScheme The authentication scheme.
     * @throws SQLException if value is invalid.
     */
    public void setAuthScheme(final AuthScheme authScheme) throws SQLException {
        if (authScheme == null) {
            throw invalidConnectionPropertyError(AUTH_SCHEME_KEY, authScheme);
        }
        put(AUTH_SCHEME_KEY, authScheme);
    }

    /**
     * Gets the use encryption.
     *
     * @return The use encryption.
     */
    public boolean getUseEncryption() {
        return (boolean) get(USE_ENCRYPTION_KEY);
    }

    /**
     * Sets the use encryption.
     *
     * @param useEncryption The use encryption.
     */
    public void setUseEncryption(final boolean useEncryption) {
        put(USE_ENCRYPTION_KEY, useEncryption);
    }

    /**
     * Gets the region.
     *
     * @return The region.
     */
    public String getRegion() {
        return getProperty(REGION_KEY);
    }

    /**
     * Sets the region.
     *
     * @param region The region.
     * @throws SQLException if value is invalid.
     */
    public void setRegion(final String region) throws SQLException {
        setProperty(REGION_KEY,
                (String) PROPERTIES_MAP.get(REGION_KEY).convert(REGION_KEY, region));
    }

    /**
     * Property converter interface.
     *
     * @param <T> Type to convert string property to.
     */
    interface PropertyConverter<T> {
        T convert(@NonNull String key, @NonNull String value) throws SQLException;
    }
}
