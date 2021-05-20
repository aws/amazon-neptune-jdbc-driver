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

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import org.apache.http.client.HttpClient;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class SparqlConnectionProperties extends ConnectionProperties {
    // URL of the Neptune endpoint (*without* the trailing "/sparql" servlet)
    // contactPoint doesn't apply to RDF builder, currently using it as the root part of the full url
    public static final String CONTACT_POINT_KEY = "rootUrl";
    public static final String PORT_KEY = "port";
    // dataset endpoint
    public static final String ENDPOINT_KEY = "endpoint";
    //    public static final String DESTINATION_KEY = "destination";
    // the query and update endpoints for sparql database
    public static final String QUERY_ENDPOINT_KEY = "queryEndpoint";
    // TODO: remove this
    public static final String UPDATE_ENDPOINT_KEY = "updateEndpoint";
    public static final String REGION_KEY = "region";
    public static final String CONNECTION_POOL_SIZE_KEY = "connectionPoolSize";
    public static final String ACCEPT_HEADER_ASK_QUERY_KEY = "acceptHeaderAskQuery";
    public static final String ACCEPT_HEADER_DATASET_KEY = "acceptHeaderDataset";
    public static final String ACCEPT_HEADER_GRAPH_KEY = "acceptHeaderGraph";
    public static final String ACCEPT_HEADER_QUERY_KEY = "acceptHeaderQuery";
    public static final String ACCEPT_HEADER_SELECT_QUERY_KEY = "acceptHeaderSelectQuery";
    public static final String GSP_ENDPOINT_KEY = "gspEndpoint";
    public static final String PARSE_CHECK_SPARQL_KEY = "parseCheckSparql";
    public static final String HTTP_CLIENT_KEY = "httpClient";
    public static final String HTTP_CONTEXT_KEY = "httpContext";
    public static final String QUADS_FORMAT_KEY = "quadsFormat";
    public static final String TRIPLES_FORMAT_KEY = "triplesFormat";
    // TODO: Revisit. We should probably support these.
    public static final String AWS_CREDENTIALS_PROVIDER_CLASS_KEY = "awsCredentialsProviderClass";
    public static final String CUSTOM_CREDENTIALS_FILE_PATH_KEY = "customCredentialsFilePath";
    public static final int DEFAULT_PORT = 3030;
    public static final int DEFAULT_CONNECTION_POOL_SIZE = 1000;
    public static final boolean DEFAULT_USE_ENCRYPTION = true;
    public static final Map<String, Object> DEFAULT_PROPERTIES_MAP = new HashMap<>();
    private static final Map<String, ConnectionProperties.PropertyConverter<?>> PROPERTY_CONVERTER_MAP =
            new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlConnectionProperties.class);
    private static final Set<String> SUPPORTED_PROPERTIES_SET = ImmutableSet.<String>builder()
            .add(CONTACT_POINT_KEY)
            .add(PORT_KEY)
            .add(ENDPOINT_KEY)
            //.add(DESTINATION_KEY)
            .add(QUERY_ENDPOINT_KEY)
            .add(UPDATE_ENDPOINT_KEY)
            .add(REGION_KEY)
            .add(CONNECTION_POOL_SIZE_KEY)
            .add(ACCEPT_HEADER_ASK_QUERY_KEY)
            .add(ACCEPT_HEADER_DATASET_KEY)
            .add(ACCEPT_HEADER_GRAPH_KEY)
            .add(ACCEPT_HEADER_QUERY_KEY)
            .add(ACCEPT_HEADER_SELECT_QUERY_KEY)
            .add(GSP_ENDPOINT_KEY)
            .add(PARSE_CHECK_SPARQL_KEY)
            .add(HTTP_CLIENT_KEY)
            .add(HTTP_CONTEXT_KEY)
            .add(QUADS_FORMAT_KEY)
            .add(TRIPLES_FORMAT_KEY)
            .add(AWS_CREDENTIALS_PROVIDER_CLASS_KEY)
            .add(CUSTOM_CREDENTIALS_FILE_PATH_KEY)
            .build();

    // property converter parses on the in-coming connection string
    static {
        PROPERTY_CONVERTER_MAP.put(CONTACT_POINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(PORT_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(ENDPOINT_KEY, (key, value) -> value);
        // PROPERTY_CONVERTER_MAP.put(DESTINATION_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(QUERY_ENDPOINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(UPDATE_ENDPOINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(REGION_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(CONNECTION_POOL_SIZE_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(PARSE_CHECK_SPARQL_KEY, ConnectionProperties::toBoolean);
        PROPERTY_CONVERTER_MAP.put(ACCEPT_HEADER_ASK_QUERY_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(ACCEPT_HEADER_DATASET_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(ACCEPT_HEADER_GRAPH_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(ACCEPT_HEADER_QUERY_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(ACCEPT_HEADER_SELECT_QUERY_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(GSP_ENDPOINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(QUADS_FORMAT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(TRIPLES_FORMAT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(AWS_CREDENTIALS_PROVIDER_CLASS_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(CUSTOM_CREDENTIALS_FILE_PATH_KEY, (key, value) -> value);
    }

    static {
        DEFAULT_PROPERTIES_MAP.put(PORT_KEY, DEFAULT_PORT);
        DEFAULT_PROPERTIES_MAP.put(CONTACT_POINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(QUERY_ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(UPDATE_ENDPOINT_KEY, "");
        // DEFAULT_PROPERTIES_MAP.put(DESTINATION_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(REGION_KEY, "");
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

    // this doesn't quite work, as it get will reset the value based on the ports and stuff everytime, so the setter
    // then has to change all 3 fields which doesn't seem reasonable --> moving this back to queryExecutor
    // @SneakyThrows
    // public String getDestination() {
    //    if (!containsKey(CONTACT_POINT_KEY) && !containsKey(PORT_KEY) && !containsKey(ENDPOINT_KEY)) {
    //        return null;
    //    }
    //    final String databaseUrl = getContactPoint() + ":" + getPort() + "/" + getEndpoint();
    //    setDestination(databaseUrl);
    //    return getProperty(DESTINATION_KEY);
    // }

    // public void setDestination(@NonNull final String destination) throws SQLException {
    //     put(DESTINATION_KEY, destination);
    // }

    /**
     * Gets the query endpoint.
     *
     * @return The query endpoint for sparql query.
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
     * Gets the update endpoint.
     *
     * @return The update endpoint for sparql query.
     */
    public String getUpdateEndpoint() {
        return getProperty(UPDATE_ENDPOINT_KEY);
    }

    /**
     * Sets the update endpoint.
     *
     * @param updateEndpoint The connection endpoint.
     * @throws SQLException if value is invalid.
     */
    public void setUpdateEndpoint(@NonNull final String updateEndpoint) throws SQLException {
        setProperty(UPDATE_ENDPOINT_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(UPDATE_ENDPOINT_KEY).convert(UPDATE_ENDPOINT_KEY, updateEndpoint));
    }

    /**
     * Gets the HTTP accept:header used when making a SPARQL Protocol ASK query.
     *
     * @return The HTTP accept:header.
     */
    public String getAcceptHeaderAskQuery() {
        return getProperty(ACCEPT_HEADER_ASK_QUERY_KEY);
    }

    /**
     * Sets the HTTP accept:header used when making a SPARQL Protocol ASK query.
     *
     * @param acceptHeaderAskQuery The HTTP endpoint.
     * @throws SQLException if value is invalid.
     */
    public void setAcceptHeaderAskQuery(@NonNull final String acceptHeaderAskQuery) throws SQLException {
        setProperty(ACCEPT_HEADER_ASK_QUERY_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(ACCEPT_HEADER_ASK_QUERY_KEY).convert(ACCEPT_HEADER_ASK_QUERY_KEY,
                        acceptHeaderAskQuery));
    }

    /**
     * Gets the HTTP accept:header used to fetch RDF dataset using HTTP GET.
     *
     * @return The HTTP accept:header.
     */
    public String getAcceptHeaderDataset() {
        return getProperty(ACCEPT_HEADER_DATASET_KEY);
    }

    /**
     * Sets the HTTP accept:header used to fetch RDF dataset using HTTP GET.
     *
     * @param acceptHeaderDataset The HTTP endpoint.
     * @throws SQLException if value is invalid.
     */
    public void setAcceptHeaderDataset(@NonNull final String acceptHeaderDataset) throws SQLException {
        setProperty(ACCEPT_HEADER_DATASET_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(ACCEPT_HEADER_DATASET_KEY).convert(ACCEPT_HEADER_DATASET_KEY,
                        acceptHeaderDataset));
    }

    /**
     * Gets the HTTP accept:header used to fetch RDF graph using SPARQL Graph Store Protocol.
     *
     * @return The HTTP accept:header.
     */
    public String getAcceptHeaderGraph() {
        return getProperty(ACCEPT_HEADER_GRAPH_KEY);
    }

    /**
     * Sets the HTTP accept:header used to fetch RDF graph using SPARQL Graph Store Protocol.
     *
     * @param acceptHeaderGraph The HTTP endpoint.
     * @throws SQLException if value is invalid.
     */
    public void setAcceptHeaderGraph(@NonNull final String acceptHeaderGraph) throws SQLException {
        setProperty(ACCEPT_HEADER_GRAPH_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(ACCEPT_HEADER_GRAPH_KEY).convert(ACCEPT_HEADER_GRAPH_KEY,
                        acceptHeaderGraph));
    }

    /**
     * Gets the HTTP accept:header used when making SPARQL Protocol query if no query specific setting is available.
     *
     * @return The HTTP accept:header.
     */
    public String getAcceptHeaderQuery() {
        return getProperty(ACCEPT_HEADER_QUERY_KEY);
    }

    /**
     * Sets the HTTP accept:header used when making SPARQL Protocol query if no query specific setting is available.
     *
     * @param acceptHeaderQuery The HTTP endpoint.
     * @throws SQLException if value is invalid.
     */
    public void setAcceptHeaderQuery(@NonNull final String acceptHeaderQuery) throws SQLException {
        setProperty(ACCEPT_HEADER_QUERY_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(ACCEPT_HEADER_QUERY_KEY).convert(ACCEPT_HEADER_QUERY_KEY,
                        acceptHeaderQuery));
    }

    /**
     * Gets the HTTP accept:header used when making SPARQL Protocol query if no query specific setting is available.
     *
     * @return The HTTP accept:header.
     */
    public String getAcceptHeaderSelectQuery() {
        return getProperty(ACCEPT_HEADER_SELECT_QUERY_KEY);
    }

    /**
     * Sets the HTTP accept:header used when making SPARQL Protocol query if no query specific setting is available.
     *
     * @param acceptHeaderSelectQuery The HTTP accept:header.
     * @throws SQLException if value is invalid.
     */
    public void setAcceptHeaderSelectQuery(@NonNull final String acceptHeaderSelectQuery) throws SQLException {
        setProperty(ACCEPT_HEADER_SELECT_QUERY_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(ACCEPT_HEADER_SELECT_QUERY_KEY)
                        .convert(ACCEPT_HEADER_SELECT_QUERY_KEY,
                                acceptHeaderSelectQuery));
    }

    /**
     * Gets the HttpClient for the connection to be built.
     *
     * @return The HttpClient
     */
    public HttpClient getHttpClient() {
        return (HttpClient) get(HTTP_CLIENT_KEY);
    }

    /**
     * Sets the HttpClient for the connection to be built.
     *
     * @param httpClient The HTTP client.
     * @throws SQLException if value is invalid.
     */
    public void setHttpClient(@NonNull final HttpClient httpClient) throws SQLException {
        put(HTTP_CLIENT_KEY, httpClient);
    }

    /**
     * Gets the HttpContext for the connection to be built.
     *
     * @return The HttpContext.
     */
    public HttpContext getHttpContext() {
        return (HttpContext) get(HTTP_CONTEXT_KEY);
    }

    /**
     * Sets the HttpContext for the connection to be built.
     *
     * @param httpContext The HTTP context.
     * @throws SQLException if value is invalid.
     */
    public void setHttpContext(@NonNull final HttpContext httpContext) throws SQLException {
        put(HTTP_CONTEXT_KEY, httpContext);
    }

    /**
     * Gets the flag for whether to check SPARQL queries and SPARQL updates provided as a string
     *
     * @return The HTTP accept:header.
     */
    public boolean getParseCheckSparql() {
        return (boolean) get(PARSE_CHECK_SPARQL_KEY);
    }

    /**
     * Sets the flag for whether to check SPARQL queries and SPARQL updates provided as a string
     *
     * @param parseCheckSparql The flag.
     * @throws SQLException if value is invalid.
     */
    public void setParseCheckSparql(final boolean parseCheckSparql) throws SQLException {
        put(PARSE_CHECK_SPARQL_KEY, parseCheckSparql);
    }

    /**
     * Gets the name of the SPARQL GraphStore Protocol endpoint
     *
     * @return The HTTP name of the SPARQL GraphStore Protocol endpoint
     */
    public String getGspEndpoint() {
        return getProperty(GSP_ENDPOINT_KEY);
    }

    /**
     * Sets thename of the SPARQL GraphStore Protocol endpoint
     *
     * @param gspEndpoint The endpoint.
     * @throws SQLException if value is invalid.
     */
    public void setGspEndpoint(@NonNull final String gspEndpoint) throws SQLException {
        setProperty(GSP_ENDPOINT_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(GSP_ENDPOINT_KEY).convert(GSP_ENDPOINT_KEY,
                        gspEndpoint));
    }


    /**
     * Gets the format for sending RDF Datasets to the remote server.
     *
     * @return The HTTP accept:header.
     */
    public String getQuadsFormat() {
        return getProperty(QUADS_FORMAT_KEY);
    }

    /**
     * Sets the format for sending RDF Datasets to the remote server.
     *
     * @param quadsFormat The flag.
     * @throws SQLException if value is invalid.
     */
    public void setQuadsFormat(@NonNull final String quadsFormat) throws SQLException {
        setProperty(QUADS_FORMAT_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(QUADS_FORMAT_KEY).convert(QUADS_FORMAT_KEY,
                        quadsFormat));
    }

    /**
     * Gets the format for sending RDF Datasets to the remote server.
     *
     * @return The HTTP accept:header.
     */
    public String getTriplesFormat() {
        return getProperty(TRIPLES_FORMAT_KEY);
    }

    /**
     * Sets the format for sending RDF Datasets to the remote server.
     *
     * @param triplesFormat The flag.
     * @throws SQLException if value is invalid.
     */
    public void setTriplesFormat(@NonNull final String triplesFormat) throws SQLException {
        setProperty(TRIPLES_FORMAT_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(TRIPLES_FORMAT_KEY).convert(TRIPLES_FORMAT_KEY,
                        triplesFormat));
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
                        "A Region must be provided to use IAMSigV4 Authentication. Set the SERVICE_REGION " +
                                "environment variable to the appropriate region, such as 'us-east-1'.");
            }
            setRegion(region);
            // TODO: also need to make a new HttpClient for this? like the example in Amazon v4SigningClient?
            //  https://github.com/aws/amazon-neptune-sparql-java-sigv4/blob/master/src/main/java/com/amazonaws/neptune/client/jena/NeptuneJenaSigV4Example.java

            // TODO: Jena RDF builder doesn't have an encryption field, do we somehow support it?
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
        return SUPPORTED_PROPERTIES_SET.contains(name);
    }
}
