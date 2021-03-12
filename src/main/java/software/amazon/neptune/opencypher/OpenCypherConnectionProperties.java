/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.neptune.opencypher;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * OpenCypher connection properties class.
 */
public class OpenCypherConnectionProperties extends ConnectionProperties {
    public static final String ENDPOINT_KEY = "Endpoint";

    public static final String AUTH_SCHEME_KEY = "AuthScheme";
    public static final String USE_ENCRYPTION_KEY = "UseEncryption";
    public static final String REGION_KEY = "Region";
    public static final String CONNECTION_POOL_SIZE_KEY = "ConnectionPoolSize";

    // TODO: Revisit. We should probably support these.
    public static final String AWS_CREDENTIALS_PROVIDER_CLASS_KEY = "AwsCredentialsProviderClass";
    public static final String CUSTOM_CREDENTIALS_FILE_PATH_KEY = "CustomCredentialsFilePath";

    public static final int DEFAULT_CONNECTION_POOL_SIZE = 1000;
    public static final AuthScheme DEFAULT_AUTH_SCHEME = AuthScheme.None;
    public static final boolean DEFAULT_USE_ENCRYPTION = true;

    public static final Map<String, Object> DEFAULT_PROPERTIES_MAP = new HashMap<>();
    private static final Map<String, ConnectionProperties.PropertyConverter<?>> PROPERTY_CONVERTER_MAP = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherConnectionProperties.class);

    static {
        PROPERTY_CONVERTER_MAP.put(AWS_CREDENTIALS_PROVIDER_CLASS_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(CUSTOM_CREDENTIALS_FILE_PATH_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(ENDPOINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(AUTH_SCHEME_KEY, OpenCypherConnectionProperties::toAuthScheme);
        PROPERTY_CONVERTER_MAP.put(REGION_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(USE_ENCRYPTION_KEY, ConnectionProperties::toBoolean);
        PROPERTY_CONVERTER_MAP.put(CONNECTION_POOL_SIZE_KEY, ConnectionProperties::toUnsigned);
    }

    static {
        DEFAULT_PROPERTIES_MAP.put(ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(AUTH_SCHEME_KEY, DEFAULT_AUTH_SCHEME);
        DEFAULT_PROPERTIES_MAP.put(REGION_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(USE_ENCRYPTION_KEY, DEFAULT_USE_ENCRYPTION);
        DEFAULT_PROPERTIES_MAP.put(CONNECTION_POOL_SIZE_KEY, DEFAULT_CONNECTION_POOL_SIZE);
    }

    /**
     * OpenCypherConnectionProperties constructor.
     */
    public OpenCypherConnectionProperties() throws SQLException {
        super();
    }

    /**
     * OpenCypherConnectionProperties constructor.
     * @param properties Properties to examine and extract key details from.
     */
    public OpenCypherConnectionProperties(final Properties properties) throws SQLException {
        super(properties, DEFAULT_PROPERTIES_MAP, PROPERTY_CONVERTER_MAP);
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
                (String) PROPERTY_CONVERTER_MAP.get(ENDPOINT_KEY).convert(ENDPOINT_KEY, endpoint));
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
                (String) PROPERTY_CONVERTER_MAP.get(AWS_CREDENTIALS_PROVIDER_CLASS_KEY)
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
                (String) PROPERTY_CONVERTER_MAP.get(CUSTOM_CREDENTIALS_FILE_PATH_KEY)
                        .convert(CUSTOM_CREDENTIALS_FILE_PATH_KEY, customCredentialsFilePath));
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
                (String) PROPERTY_CONVERTER_MAP.get(REGION_KEY).convert(REGION_KEY, region));
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
    protected void validateProperties() throws SQLException {
        // If IAMSigV4 is specified, we need the region provided to us.
        if (getAuthScheme() != null && getAuthScheme().equals(AuthScheme.IAMSigV4)) {
            final String region = System.getenv().get("SERVICE_REGION");
            if (region == null) {
                throw missingConnectionPropertyError("A Region must be provided to use IAMSigV4 Authentication. Set the SERVICE_REGION environment variable to the appropriate region, such as 'us-east-1'.");
            }
            setRegion(region);

            if (!getUseEncryption()) {
                throw invalidConnectionPropertyValueError(USE_ENCRYPTION_KEY,
                        "Encryption must be enabled if IAMSigV4 is used.");
            }
        }
    }

    /**
     * Check if the property is supported by the driver.
     *
     * @param name The name of the property.
     * @return {@code true} if property is supported; {@code false} otherwise.
     */
    public boolean isSupportedProperty(final String name) {
        return containsKey(name);
    }

    private static AuthScheme toAuthScheme(@NonNull final String key, @NonNull final String value) throws SQLException {
        if (isWhitespace(value)) {
            return DEFAULT_AUTH_SCHEME;
        }
        if (AuthScheme.fromString(value) == null) {
            throw invalidConnectionPropertyError(key, value);
        }
        return AuthScheme.fromString(value);
    }
}
