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

package software.aws.neptune.opencypher;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.jdbc.Connection;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import software.aws.neptune.jdbc.utilities.SqlError;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * OpenCypher connection properties class.
 */
public class OpenCypherConnectionProperties extends ConnectionProperties {
    public static final String ENDPOINT_KEY = "endpoint";
    public static final String USE_ENCRYPTION_KEY = "useEncryption";
    public static final String CONNECTION_POOL_SIZE_KEY = "connectionPoolSize";

    // TODO: Revisit. We should probably support these.
    public static final String AWS_CREDENTIALS_PROVIDER_CLASS_KEY = "awsCredentialsProviderClass";
    public static final String CUSTOM_CREDENTIALS_FILE_PATH_KEY = "customCredentialsFilePath";

    public static final int DEFAULT_CONNECTION_POOL_SIZE = 1000;
    public static final boolean DEFAULT_USE_ENCRYPTION = true;

    public static final Map<String, Object> DEFAULT_PROPERTIES_MAP = new HashMap<>();
    private static final Map<String, ConnectionProperties.PropertyConverter<?>> PROPERTY_CONVERTER_MAP =
            new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherConnectionProperties.class);

    static {
        PROPERTY_CONVERTER_MAP.put(AWS_CREDENTIALS_PROVIDER_CLASS_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(CUSTOM_CREDENTIALS_FILE_PATH_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(ENDPOINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(SERVICE_REGION_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(USE_ENCRYPTION_KEY, ConnectionProperties::toBoolean);
        PROPERTY_CONVERTER_MAP.put(CONNECTION_POOL_SIZE_KEY, ConnectionProperties::toUnsigned);
    }

    static {
        DEFAULT_PROPERTIES_MAP.put(ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(USE_ENCRYPTION_KEY, DEFAULT_USE_ENCRYPTION);
        DEFAULT_PROPERTIES_MAP.put(CONNECTION_POOL_SIZE_KEY, DEFAULT_CONNECTION_POOL_SIZE);
    }

    /**
     * OpenCypherConnectionProperties constructor.
     */
    public OpenCypherConnectionProperties() throws SQLException {
        super(new Properties(), DEFAULT_PROPERTIES_MAP, PROPERTY_CONVERTER_MAP);
    }

    /**
     * OpenCypherConnectionProperties constructor.
     *
     * @param properties Properties to examine and extract key details from.
     */
    public OpenCypherConnectionProperties(final Properties properties) throws SQLException {
        super(properties, DEFAULT_PROPERTIES_MAP, PROPERTY_CONVERTER_MAP);
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

    protected boolean isEncryptionEnabled() {
        return getUseEncryption();
    }

    private URI getUri() throws SQLException {
        try {
            return new URI(getEndpoint());
        } catch (final URISyntaxException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String getHostname() throws SQLException {
        return getUri().getHost();
    }

    @Override
    public int getPort() throws SQLException {
        return getUri().getPort();
    }

    @Override
    public void sshTunnelOverride(final int port) throws SQLException {
        setEndpoint(String.format("%s://%s:%d", getUri().getScheme(), getHostname(), port));
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
    public void setEndpoint(@NonNull final String endpoint) throws SQLException {
        setProperty(ENDPOINT_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(ENDPOINT_KEY).convert(ENDPOINT_KEY, endpoint));
    }

    /**
     * Gets the AWS credentials provider class.
     *
     * @return The AWS credentials provider class.
     */
    public String getAwsCredentialsProviderClass() {
        if (!containsKey(AWS_CREDENTIALS_PROVIDER_CLASS_KEY)) {
            return null;
        }
        return getProperty(AWS_CREDENTIALS_PROVIDER_CLASS_KEY);
    }

    /**
     * Sets the AWS credentials provider class.
     *
     * @param awsCredentialsProviderClass The AWS credentials provider class.
     * @throws SQLException if value is invalid.
     */
    public void setAwsCredentialsProviderClass(@NonNull final String awsCredentialsProviderClass) throws SQLException {
        setProperty(AWS_CREDENTIALS_PROVIDER_CLASS_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(AWS_CREDENTIALS_PROVIDER_CLASS_KEY)
                        .convert(AWS_CREDENTIALS_PROVIDER_CLASS_KEY, awsCredentialsProviderClass));
    }

    /**
     * Gets the custom credentials filepath.
     *
     * @return The custom credentials filepath.
     */
    public String getCustomCredentialsFilePath() {
        if (!containsKey(CUSTOM_CREDENTIALS_FILE_PATH_KEY)) {
            return null;
        }
        return getProperty(CUSTOM_CREDENTIALS_FILE_PATH_KEY);
    }

    /**
     * Sets the custom credentials filepath.
     *
     * @param customCredentialsFilePath The custom credentials filepath.
     * @throws SQLException if value is invalid.
     */
    public void setCustomCredentialsFilePath(@NonNull final String customCredentialsFilePath) throws SQLException {
        setProperty(CUSTOM_CREDENTIALS_FILE_PATH_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(CUSTOM_CREDENTIALS_FILE_PATH_KEY)
                        .convert(CUSTOM_CREDENTIALS_FILE_PATH_KEY, customCredentialsFilePath));
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
    public void setUseEncryption(final boolean useEncryption) throws SQLClientInfoException {
        if (!useEncryption && getAuthScheme().equals(AuthScheme.IAMSigV4)) {
            throw SqlError.createSQLClientInfoException(
                    LOGGER,
                    Connection.getFailures("useEncrpytion", "true"),
                    SqlError.INVALID_CONNECTION_PROPERTY, "useEncrpytion",
                    "'false' when authScheme is set to 'IAMSigV4'");
        }
        put(USE_ENCRYPTION_KEY, useEncryption);
    }

    /**
     * Gets the connection pool size.
     *
     * @return The connection pool size.
     */
    public int getConnectionPoolSize() {
        return (int) get(CONNECTION_POOL_SIZE_KEY);
    }

    /**
     * Sets the connection pool size.
     *
     * @param connectionPoolSize The connection pool size.
     * @throws SQLException if value is invalid.
     */
    public void setConnectionPoolSize(final int connectionPoolSize) throws SQLException {
        if (connectionPoolSize < 0) {
            throw invalidConnectionPropertyError(CONNECTION_POOL_SIZE_KEY, connectionPoolSize);
        }
        put(CONNECTION_POOL_SIZE_KEY, connectionPoolSize);
    }

    /**
     * Validate the supported properties.
     */
    @Override
    protected void validateProperties() throws SQLException {
        // If IAMSigV4 is specified, we need the region provided to us.
        if (getAuthScheme() != null && getAuthScheme().equals(AuthScheme.IAMSigV4)) {
            if ("".equals(getRegion())) {
                if (System.getenv("SERVICE_REGION") == null) {
                    throw missingConnectionPropertyError(
                            "A Service Region must be provided to use IAMSigV4 Authentication through " +
                                    "the SERVICE_REGION environment variable or the serviceRegion connection property. " +
                                    "For example, append 'serviceRegion=us-east-1;' to your connection string");
                }
                LOGGER.warn("serviceRegion property was not set, using system SERVICE_REGION environment variable");
                setRegion(System.getenv("SERVICE_REGION"));
            }

            if (!getUseEncryption()) {
                throw invalidConnectionPropertyValueError(USE_ENCRYPTION_KEY,
                        "Encryption must be enabled if IAMSigV4 is used");
            }
        }
    }

    /**
     * Check if the property is supported by the driver.
     *
     * @param name The name of the property.
     * @return {@code true} if property is supported; {@code false} otherwise.
     */
    @Override
    public boolean isSupportedProperty(final String name) {
        return containsKey(name);
    }
}
