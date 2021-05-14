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

package software.amazon.neptune.sparql;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SparqlConnectionProperties extends ConnectionProperties {

    // reference https://github.com/aws/amazon-neptune-sparql-java-sigv4/blob/master/src/main/java/com/amazonaws/neptune/client/rdf4j/NeptuneSparqlRepository.java

    // URL of the Neptune endpoint (*without* the trailing "/sparql" servlet)
    // contactPoint doesn't apply to RDF builder, currently using it as the root part of the full url
    public static final String CONTACT_POINT_KEY = "rooUrl";
    public static final String PORT_KEY = "port";
    public static final int DEFAULT_PORT = 3030;
    // equivalent to dataset
    public static final String ENDPOINT_KEY = "endpoint";
    // the two endpoints for sparql database
    public static final String QUERY_ENDPOINT_KEY = "queryEndpoint";
    // TODO: we won't support update operations, but leaving it here for now
    public static final String UPDATE_ENDPOINT_KEY = "updateEndpoint";
    // does this have anything to do with authenticationEnabled?
    // sparql might not have this as a input?
    public static final String USE_ENCRYPTION_KEY = "useEncryption";
    // The name of the region in which Neptune is running.
    public static final String REGION_KEY = "region";
    public static final String CONNECTION_POOL_SIZE_KEY = "connectionPoolSize";

    // TODO: Revisit. We should probably support these.
    // Credentials for signing request? - not supporting these?
    public static final String AWS_CREDENTIALS_PROVIDER_CLASS_KEY = "awsCredentialsProviderClass";
    public static final String CUSTOM_CREDENTIALS_FILE_PATH_KEY = "customCredentialsFilePath";

    public static final int DEFAULT_CONNECTION_POOL_SIZE = 1000;
    public static final boolean DEFAULT_USE_ENCRYPTION = true;

    public static final Map<String, Object> DEFAULT_PROPERTIES_MAP = new HashMap<>();
    private static final Map<String, ConnectionProperties.PropertyConverter<?>> PROPERTY_CONVERTER_MAP =
            new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlConnectionProperties.class);

    static {
        PROPERTY_CONVERTER_MAP.put(CONTACT_POINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(AWS_CREDENTIALS_PROVIDER_CLASS_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(CUSTOM_CREDENTIALS_FILE_PATH_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(PORT_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(ENDPOINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(QUERY_ENDPOINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(UPDATE_ENDPOINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(REGION_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(USE_ENCRYPTION_KEY, ConnectionProperties::toBoolean);
        PROPERTY_CONVERTER_MAP.put(CONNECTION_POOL_SIZE_KEY, ConnectionProperties::toUnsigned);
    }

    static {
        DEFAULT_PROPERTIES_MAP.put(PORT_KEY, DEFAULT_PORT);
        DEFAULT_PROPERTIES_MAP.put(CONTACT_POINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(QUERY_ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(UPDATE_ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(REGION_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(USE_ENCRYPTION_KEY, DEFAULT_USE_ENCRYPTION);
        DEFAULT_PROPERTIES_MAP.put(CONNECTION_POOL_SIZE_KEY, DEFAULT_CONNECTION_POOL_SIZE);
    }

    /**
     * SparqlConnectionProperties constructor.
     */
    public SparqlConnectionProperties() throws SQLException {
        super(new Properties(), DEFAULT_PROPERTIES_MAP, PROPERTY_CONVERTER_MAP);
    }

    /**
     * SparqlConnectionProperties constructor.
     *
     * @param properties Properties to examine and extract key details from.
     */
    public SparqlConnectionProperties(final Properties properties) throws SQLException {
        super(properties, DEFAULT_PROPERTIES_MAP, PROPERTY_CONVERTER_MAP);
    }

    protected static AuthScheme toAuthScheme(@NonNull final String key, @NonNull final String value) throws SQLException {
        if (isWhitespace(value)) {
            return DEFAULT_AUTH_SCHEME;
        }
        if (AuthScheme.fromString(value) == null) {
            throw invalidConnectionPropertyError(key, value);
        }
        return AuthScheme.fromString(value);
    }

    /**
     * Gets the connection contact point.
     *
     * @return The connection contact point.
     */
    public String getContactPoint() {
        return getProperty(CONTACT_POINT_KEY);
    }

    /**
     * Sets the connection contact point.
     *
     * @param contactPoint The connection contact point.
     * @throws SQLException if value is invalid.
     */
    public void setContactPoint(@NonNull final String contactPoint) throws SQLException {
        setProperty(CONTACT_POINT_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(CONTACT_POINT_KEY).convert(CONTACT_POINT_KEY, contactPoint));
    }

    /**
     * Gets the port that the Gremlin Servers will be listening on.
     *
     * @return The port.
     */
    public int getPort() {
        return (int) get(PORT_KEY);
    }

    /**
     * Sets the port that the Gremlin Servers will be listening on.
     *
     * @param port The port.
     */
    public void setPort(final int port) throws SQLException {
        if (port < 0) {
            throw invalidConnectionPropertyError(PORT_KEY, port);
        }
        put(PORT_KEY, port);
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
     * Gets the connection endpoint.
     *
     * @return The connection endpoint for sparql query.
     */
    public String getQueryEndpoint() {
        return getProperty(QUERY_ENDPOINT_KEY);
    }

    /**
     * Sets the connection endpoint.
     *
     * @param queryEndpoint The connection endpoint.
     * @throws SQLException if value is invalid.
     */
    public void setQueryEndpoint(@NonNull final String queryEndpoint) throws SQLException {
        setProperty(QUERY_ENDPOINT_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(QUERY_ENDPOINT_KEY).convert(QUERY_ENDPOINT_KEY, queryEndpoint));
    }

    /**
     * Gets the connection endpoint.
     *
     * @return The connection endpoint for sparql query.
     */
    public String getUpdateEndpoint() {
        return getProperty(UPDATE_ENDPOINT_KEY);
    }

    /**
     * Sets the connection endpoint.
     *
     * @param updateEndpoint The connection endpoint.
     * @throws SQLException if value is invalid.
     */
    public void setUpdateEndpoint(@NonNull final String updateEndpoint) throws SQLException {
        setProperty(UPDATE_ENDPOINT_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(UPDATE_ENDPOINT_KEY).convert(UPDATE_ENDPOINT_KEY, updateEndpoint));
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
    @Override
    protected void validateProperties() throws SQLException {
        // If IAMSigV4 is specified, we need the region provided to us.
        if (getAuthScheme() != null && getAuthScheme().equals(AuthScheme.IAMSigV4)) {
            final String region = System.getenv().get("SERVICE_REGION");
            if (region == null) {
                throw missingConnectionPropertyError(
                        "A Region must be provided to use IAMSigV4 Authentication. Set the SERVICE_REGION environment variable to the appropriate region, such as 'us-east-1'.");
            }
            setRegion(region);

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
