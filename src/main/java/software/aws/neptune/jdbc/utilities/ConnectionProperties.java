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

package software.aws.neptune.jdbc.utilities;

import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class that contains connection properties.
 */
public abstract class ConnectionProperties extends Properties {
    public static final String APPLICATION_NAME_KEY = "ApplicationName";
    public static final String AUTH_SCHEME_KEY = "AuthScheme";
    public static final String CONNECTION_TIMEOUT_MILLIS_KEY = "ConnectionTimeout";
    public static final String CONNECTION_RETRY_COUNT_KEY = "ConnectionRetryCount";
    public static final String LOG_LEVEL_KEY = "LogLevel";
    public static final String SSH_USER = "sshUser";
    public static final String SSH_HOSTNAME = "sshHost";
    public static final String SSH_PRIVATE_KEY_FILE = "sshPrivateKeyFile";
    public static final String SSH_PRIVATE_KEY_PASSPHRASE = "sshPrivateKeyPassphrase";
    public static final String SSH_STRICT_HOST_KEY_CHECKING = "sshStrictHostKeyChecking";
    public static final String SSH_KNOWN_HOSTS_FILE = "sshKnownHostsFile";
    public static final AuthScheme DEFAULT_AUTH_SCHEME = AuthScheme.IAMSigV4;
    public static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 5000;
    public static final int DEFAULT_CONNECTION_RETRY_COUNT = 3;
    public static final String DEFAULT_SSH_STRICT_CHECKING = "true";
    public static final Level DEFAULT_LOG_LEVEL = Level.OFF;

    public static final Map<String, Object> DEFAULT_PROPERTIES_MAP = new HashMap<>();
    private static final Map<String, ConnectionProperties.PropertyConverter<?>> PROPERTY_CONVERTER_MAP =
            new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProperties.class);

    static {
        PROPERTY_CONVERTER_MAP.put(APPLICATION_NAME_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(AUTH_SCHEME_KEY, ConnectionProperties::toAuthScheme);
        PROPERTY_CONVERTER_MAP.put(CONNECTION_TIMEOUT_MILLIS_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(CONNECTION_RETRY_COUNT_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(LOG_LEVEL_KEY, ConnectionProperties::toLogLevel);
        PROPERTY_CONVERTER_MAP.put(SSH_USER, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(SSH_HOSTNAME, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(SSH_PRIVATE_KEY_FILE, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(SSH_PRIVATE_KEY_PASSPHRASE, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(SSH_STRICT_HOST_KEY_CHECKING, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(SSH_KNOWN_HOSTS_FILE, (key, value) -> value);
    }

    static {
        DEFAULT_PROPERTIES_MAP.put(CONNECTION_TIMEOUT_MILLIS_KEY, DEFAULT_CONNECTION_TIMEOUT_MILLIS);
        DEFAULT_PROPERTIES_MAP.put(CONNECTION_RETRY_COUNT_KEY, DEFAULT_CONNECTION_RETRY_COUNT);
        DEFAULT_PROPERTIES_MAP.put(AUTH_SCHEME_KEY, DEFAULT_AUTH_SCHEME);
        DEFAULT_PROPERTIES_MAP.put(LOG_LEVEL_KEY, DEFAULT_LOG_LEVEL);
    }

    /**
     * ConnectionProperties constructor.
     */
    public ConnectionProperties() throws SQLException {
        this(new Properties(), null, null);
    }

    /**
     * ConnectionProperties constructor.
     *
     * @param properties initial set of connection properties coming from the connection string.
     */
    public ConnectionProperties(@NonNull final Properties properties,
                                final Map<String, Object> defaultPropertiesMap,
                                final Map<String, ConnectionProperties.PropertyConverter<?>> propertyConverterMap)
            throws SQLException {

        if (defaultPropertiesMap != null) {
            DEFAULT_PROPERTIES_MAP.putAll(defaultPropertiesMap);
        }

        if (propertyConverterMap != null) {
            PROPERTY_CONVERTER_MAP.putAll(propertyConverterMap);
        }

        if (properties.isEmpty()) {
            putAll(DEFAULT_PROPERTIES_MAP);
            return;
        }

        resolveProperties(properties);
    }

    protected static Level toLogLevel(@NonNull final String key, @NonNull final String value) throws SQLException {
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
    }

    protected static int toUnsigned(@NonNull final String key, @NonNull final String value) throws SQLException {
        if (isWhitespace(value)) {
            if (DEFAULT_PROPERTIES_MAP.containsKey(key)) {
                return (int) DEFAULT_PROPERTIES_MAP.get(key);
            } else {
                throw invalidConnectionPropertyError(key, value);
            }
        }
        try {
            final int intValue = Integer.parseUnsignedInt(value);
            if (intValue < 0) {
                throw invalidConnectionPropertyError(key, value);
            }
            return intValue;
        } catch (final NumberFormatException | SQLException e) {
            throw invalidConnectionPropertyError(key, value);
        }
    }

    protected static boolean toBoolean(@NonNull final String key, @NonNull final String value) throws SQLException {
        if (isWhitespace(value)) {
            if (DEFAULT_PROPERTIES_MAP.containsKey(key)) {
                return (boolean) DEFAULT_PROPERTIES_MAP.get(key);
            } else {
                throw invalidConnectionPropertyError(key, value);
            }
        }
        final Map<String, Boolean> stringBooleanMap = ImmutableMap.of(
                "1", true, "true", true,
                "0", false, "false", false);
        if (!stringBooleanMap.containsKey(value.toLowerCase())) {
            throw invalidConnectionPropertyError(key, value);
        }
        return stringBooleanMap.get(value.toLowerCase());
    }

    protected static AuthScheme toAuthScheme(@NonNull final String key, @NonNull final String value)
            throws SQLException {
        if (isWhitespace(value)) {
            return DEFAULT_AUTH_SCHEME;
        }
        if (AuthScheme.fromString(value) == null) {
            throw invalidConnectionPropertyError(key, value);
        }
        return AuthScheme.fromString(value);
    }

    protected static boolean isWhitespace(@NonNull final String value) {
        return Pattern.matches("^\\s*$", value);
    }

    protected static SQLException invalidConnectionPropertyError(final Object key, final Object value) {
        return SqlError.createSQLException(
                LOGGER,
                SqlState.CONNECTION_EXCEPTION,
                SqlError.INVALID_CONNECTION_PROPERTY, key, value);
    }

    protected static SQLException missingConnectionPropertyError(final String reason) {
        return SqlError.createSQLException(
                LOGGER,
                SqlState.CONNECTION_EXCEPTION,
                SqlError.MISSING_CONNECTION_PROPERTY, reason);
    }

    protected static SQLException invalidConnectionPropertyValueError(final String key, final String reason) {
        return SqlError.createSQLException(
                LOGGER,
                SqlState.CONNECTION_EXCEPTION,
                SqlError.INVALID_VALUE_CONNECTION_PROPERTY, key, reason);
    }

    /**
     * Gets an absolute path from the given file path. It performs the substitution for a leading
     * '~' to be replaced by the user's home directory.
     *
     * @param filePath the given file path to process.
     * @return a {@link Path} for the absolution path for the given file path.
     */
    public static Path getPath(final String filePath) {
        if (filePath.matches(SshTunnel.HOME_PATH_PREFIX_REG_EXPR)) {
            final String userHomePath = Matcher.quoteReplacement(
                    System.getProperty(SshTunnel.USER_HOME_PROPERTY));
            return Paths.get(filePath.replaceFirst("~", userHomePath)).toAbsolutePath();
        }
        return Paths.get(filePath).toAbsolutePath();
    }

    /**
     * Function to get the hostname.
     *
     * @return hostname.
     * @throws SQLException If the hostname cannot be obtained.
     */
    public abstract String getHostname() throws SQLException;

    /**
     * Function to get the port.
     *
     * @return port.
     * @throws SQLException If the port cannot be obatined.
     */
    public abstract int getPort() throws SQLException;

    /**
     * Function to override the current port with the ssh tunnel port.
     *
     * @param port Port to override with.
     * @throws SQLException If the override fails.
     */
    public abstract void sshTunnelOverride(int port) throws SQLException;

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
    public void setApplicationName(@NonNull final String applicationName) throws SQLException {
        setProperty(APPLICATION_NAME_KEY, applicationName);
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
    public void setAuthScheme(@NonNull final AuthScheme authScheme) throws SQLException {
        put(AUTH_SCHEME_KEY, authScheme);
    }

    /**
     * Gets indicator of whether the options indicate the SSH port forwarding tunnel
     * should be enabled.
     *
     * @return {@code true} if the SSH port forwarding tunnel should be enabled,
     * otherwise {@code false}.
     */
    public boolean enableSshTunnel() {
        return !StringUtils.isBlank(getSshUser())
                && !StringUtils.isBlank(getSshHostname())
                && !StringUtils.isBlank(getSshPrivateKeyFile())
                && Files.exists(getPath(getSshPrivateKeyFile()));
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
     * Validate properties.
     */
    protected abstract void validateProperties() throws SQLException;

    /**
     * Check if the property is supported by the driver.
     *
     * @param name The name of the property.
     * @return {@code true} if property is supported; {@code false} otherwise.
     */
    public abstract boolean isSupportedProperty(final String name);

    /**
     * Gets the entire set of properties.
     *
     * @return The entire set of properties.
     */
    public Properties getProperties() {
        final Properties newProperties = new Properties();
        newProperties.putAll(this);
        return newProperties;
    }

    /**
     * Resolves a property and sets its value.
     *
     * @param name  The name of the property.
     * @param value The value of the property.
     * @throws SQLException If the property name or value is invalid.
     */
    public void validateAndSetProperty(final String name, final Object value) throws SQLException {
        final Properties properties = new Properties();
        properties.put(name, value);
        resolveProperties(properties);
    }

    /**
     * Resolves input properties and converts them into the valid set of properties.
     *
     * @param inputProperties map of properties coming from the connection string.
     * @throws SQLException if invalid input property name or value is detected.
     */
    private void resolveProperties(final Properties inputProperties) throws SQLException {
        // List of input properties keys used to keep track of unresolved properties.
        final Set<Object> inputPropertiesKeys = new HashSet<>(inputProperties.keySet());

        for (final String mapKey : PROPERTY_CONVERTER_MAP.keySet()) {
            for (final Map.Entry<Object, Object> entry : inputProperties.entrySet()) {
                final String key = entry.getKey().toString();
                final String value = entry.getValue().toString();
                // Find matching property by comparing keys (case-insensitive)
                if (key.equalsIgnoreCase(mapKey)) {
                    // Insert resolved property into the map.
                    put(mapKey, PROPERTY_CONVERTER_MAP.get(mapKey).convert(key, value));
                    // Remove key for the resolved property.
                    inputPropertiesKeys.remove(key);
                    break;
                }
            }
        }

        setDefaults();

        // Go through properties in the supportedProperties
        final Set<Object> inputPropertiesKeyCopy = new HashSet<>(inputPropertiesKeys);
        for (final Object inputPropertiesKey : inputPropertiesKeyCopy) {
            if (isSupportedProperty(inputPropertiesKey.toString())) {
                put(inputPropertiesKey, inputProperties.get(inputPropertiesKey));
                inputPropertiesKeys.remove(inputPropertiesKey);
            }
        }

        // If there are any unresolved properties left, log a warning.
        if (!inputPropertiesKeys.isEmpty()) {
            for (final Object property : inputPropertiesKeys) {
                LOGGER.warn(
                        String.format("Property '%s' is not supported by the connection string.", property.toString()));
            }
        }

        validateProperties();
    }

    void setDefaults() {
        for (final String key : DEFAULT_PROPERTIES_MAP.keySet()) {
            if (get(key) == null) {
                put(key, DEFAULT_PROPERTIES_MAP.get(key));
            }
        }
    }

    /**
     * Gets the SSH tunnel user.
     *
     * @return the SSH tunnel user.
     */
    public String getSshUser() {
        return getProperty(SSH_USER);
    }

    /**
     * Sets the SSH tunnel user.
     *
     * @param sshUser the SSH tunnel user.
     */
    public void setSshUser(final String sshUser) {
        setProperty(SSH_USER, sshUser);
    }

    /**
     * Gets the SSH tunnel host name and optional port number.
     *
     * @return the SSH tunnel host name and optional port number.
     */
    public String getSshHostname() {
        return getProperty(SSH_HOSTNAME);
    }

    /**
     * Sets the SSH tunnel host name. Can optionally contain the port number using `host-name:port'
     * syntax. If port is not provided, port 22 is assumed.
     *
     * @param sshHostname the SSH tunnel host name and optional port number.
     */
    public void setSshHostname(final String sshHostname) {
        setProperty(SSH_HOSTNAME, sshHostname);
    }

    /**
     * Gets the file path of the private key file.
     *
     * @return the file path of the private key file.
     */
    public String getSshPrivateKeyFile() {
        return getProperty(SSH_PRIVATE_KEY_FILE);
    }

    /**
     * Sets the file path of the private key file. Can be prefixed with '~' to indicate the
     * current user's home directory.
     *
     * @param sshPrivateKeyFile the file path of the private key file.
     */
    public void setSshPrivateKeyFile(final String sshPrivateKeyFile) {
        setProperty(SSH_PRIVATE_KEY_FILE, sshPrivateKeyFile);
    }

    /**
     * Gets the passphrase of the private key file.
     *
     * @return the passphrase of the private key file
     */
    public String getSshPrivateKeyPassphrase() {
        return getProperty(SSH_PRIVATE_KEY_PASSPHRASE);
    }

    /**
     * Sets the passphrase of the private key file. If not set, no passphrase will be used.
     *
     * @param sshPrivateKeyPassphrase the passphrase of the private key file
     */
    public void setSshPrivateKeyPassphrase(final String sshPrivateKeyPassphrase) {
        setProperty(SSH_PRIVATE_KEY_PASSPHRASE, sshPrivateKeyPassphrase);
    }

    /**
     * Gets the indicator for whether the SSH tunnel will perform strict host key checking.
     *
     * @return the indicator for whether the SSH tunnel will perform strict host key checking.
     */
    public boolean getSshStrictHostKeyChecking() {
        return Boolean.parseBoolean(getProperty(SSH_STRICT_HOST_KEY_CHECKING, DEFAULT_SSH_STRICT_CHECKING));
    }

    /**
     * Sets the indicator for whether the SSH tunnel will perform strict host key checking. When
     * {@code true}, the 'known_hosts' file is checked to ensure the hashed host key is the same
     * as the target host.
     *
     * @param sshStrictHostKeyChecking the indicator for whether the SSH tunnel will perform strict
     *                                 host key checking.
     */
    public void setSshStrictHostKeyChecking(final String sshStrictHostKeyChecking) {
        setProperty(
                SSH_STRICT_HOST_KEY_CHECKING,
                String.valueOf(Boolean.parseBoolean(sshStrictHostKeyChecking)));
    }

    /**
     * Gets the file path to the 'known_hosts' file.
     *
     * @return the file path to the 'known_hosts' file.
     */
    public String getSshKnownHostsFile() {
        return getProperty(SSH_KNOWN_HOSTS_FILE);
    }

    /**
     * Gets the file path to the 'known_hosts' file. If not set, '~/.ssh/known_hosts' is assumed.
     *
     * @param sshKnownHostsFile the file path to the 'known_hosts' file.
     */
    public void setSshKnownHostsFile(final String sshKnownHostsFile) {
        setProperty(SSH_KNOWN_HOSTS_FILE, sshKnownHostsFile);
    }

    /**
     * Property converter interface.
     *
     * @param <T> Type to convert string property to.
     */
    protected interface PropertyConverter<T> {
        T convert(@NonNull String key, @NonNull String value) throws SQLException;
    }
}
