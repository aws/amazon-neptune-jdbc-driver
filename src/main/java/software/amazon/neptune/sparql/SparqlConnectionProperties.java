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
    // currently this require the full url with "http://" or "https://"
    // e.g. enter "https://your-neptune-endpoint"
    public static final String ENDPOINT_KEY = "endpointURL";
    public static final String PORT_KEY = "port";
    // Dataset path with is optional depending on server (e.g. Neptune vs Fuseki)
    public static final String DATASET_KEY = "dataset";
    public static final String DESTINATION_KEY = "destination";
    // The query endpoints for sparql database
    // as a read-only driver we don't support update or the graph store protocol endpoints
    public static final String QUERY_ENDPOINT_KEY = "queryEndpoint";
    public static final String REGION_KEY = "region";
    // RDF Connection builder has the default: "application/sparql-results+json, application/sparql-results+xml;q=0.9,
    // text/tab-separated-values;q=0.7, text/csv;q=0.5, application/json;q=0.2, application/xml;q=0.2, */*;q=0.1"
    public static final String ACCEPT_HEADER_QUERY_KEY = "acceptHeaderQuery";
    public static final String ACCEPT_HEADER_ASK_QUERY_KEY = "acceptHeaderAskQuery";
    public static final String ACCEPT_HEADER_SELECT_QUERY_KEY = "acceptHeaderSelectQuery";
    public static final String PARSE_CHECK_SPARQL_KEY = "parseCheckSparql";
    public static final String ACCEPT_HEADER_DATASET_KEY = "acceptHeaderDataset";
    public static final String HTTP_CLIENT_KEY = "httpClient";
    public static final String HTTP_CONTEXT_KEY = "httpContext";
    public static final int DEFAULT_PORT = 8182; // Neptune default port
    public static final Map<String, Object> DEFAULT_PROPERTIES_MAP = new HashMap<>();
    private static final Map<String, ConnectionProperties.PropertyConverter<?>> PROPERTY_CONVERTER_MAP =
            new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlConnectionProperties.class);
    private static final Set<String> SUPPORTED_PROPERTIES_SET = ImmutableSet.<String>builder()
            .add(ENDPOINT_KEY)
            .add(PORT_KEY)
            .add(DATASET_KEY)
            .add(DESTINATION_KEY)
            .add(QUERY_ENDPOINT_KEY)
            .add(REGION_KEY)
            .add(ACCEPT_HEADER_ASK_QUERY_KEY)
            .add(ACCEPT_HEADER_DATASET_KEY)
            .add(ACCEPT_HEADER_QUERY_KEY)
            .add(ACCEPT_HEADER_SELECT_QUERY_KEY)
            .add(PARSE_CHECK_SPARQL_KEY)
            .add(HTTP_CLIENT_KEY)
            .add(HTTP_CONTEXT_KEY)
            .build();

    // property converter parses on the in-coming connection string
    static {
        PROPERTY_CONVERTER_MAP.put(ENDPOINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(PORT_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(DATASET_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(DESTINATION_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(QUERY_ENDPOINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(REGION_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(PARSE_CHECK_SPARQL_KEY, ConnectionProperties::toBoolean);
        PROPERTY_CONVERTER_MAP.put(ACCEPT_HEADER_ASK_QUERY_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(ACCEPT_HEADER_DATASET_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(ACCEPT_HEADER_QUERY_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(ACCEPT_HEADER_SELECT_QUERY_KEY, (key, value) -> value);
    }

    static {
        DEFAULT_PROPERTIES_MAP.put(PORT_KEY, DEFAULT_PORT);
        DEFAULT_PROPERTIES_MAP.put(ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(DATASET_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(QUERY_ENDPOINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(DESTINATION_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(REGION_KEY, "");
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
        return getProperty(ENDPOINT_KEY);
    }

    /**
     * Sets the connection contact point.
     *
     * @param contactPoint The connection contact point.
     * @throws SQLException if value is invalid.
     */
    public void setContactPoint(@NonNull final String contactPoint) throws SQLException {
        setProperty(ENDPOINT_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(ENDPOINT_KEY).convert(ENDPOINT_KEY, contactPoint));
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
     * Gets the dataset path for the connection string.
     *
     * @return The dataset path for the connection string.
     */
    public String getDataset() {
        return getProperty(DATASET_KEY);
    }

    /**
     * Sets the dataset path for the connection string.
     *
     * @param dataset The dataset path for the connection string.
     * @throws SQLException if value is invalid.
     */
    public void setDataset(@NonNull final String dataset) throws SQLException {
        setProperty(DATASET_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(DATASET_KEY).convert(DATASET_KEY, dataset));
    }

    /**
     * Gets the RDF connection destination.
     *
     * @return The RDF connection destination.
     */

    public String getDestination() {
        return getProperty(DESTINATION_KEY);
    }

    /**
     * Sets the RDF connection destination.
     *
     * @param destination The RDF connection destination.
     * @throws SQLException if value is invalid.
     */
    public void setDestination(@NonNull final String destination) throws SQLException {
        put(DESTINATION_KEY, destination);
    }

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
     * Validate the supported properties.
     */
    @Override
    protected void validateProperties() throws SQLException {
        // If IAMSigV4 is specified, we need the region provided to us.
        if (AuthScheme.IAMSigV4.equals(getAuthScheme())) {
            if ("".equals(getRegion())) {
                final String region = System.getenv().get("SERVICE_REGION");
                if (region == null) {
                    throw missingConnectionPropertyError(
                            "A Region must be provided to use IAMSigV4 Authentication. Set the SERVICE_REGION " +
                                    "environment variable to the appropriate region, such as 'us-east-1'.");
                }
                setRegion(region);
            }
            // Throw if both IAM AUTH and HTTP_CLIENT_KEY are given
            if (getHttpClient() != null) {
                throw invalidConnectionPropertyValueError(AUTH_SCHEME_KEY, "IAMSigV4 does not support custom" +
                        "HttpClient input. Set AuthScheme to None to pass in custom HttpClient.");
            }
        }

        if ("".equals(getContactPoint()) || getPort() < 0) {
            throw missingConnectionPropertyError("The CONTACT_POINT and PORT_KEY fields must be" +
                    " provided");
        }

        String destination = String.format("%s:%d", getContactPoint(), getPort());

        if (!"".equals(getDataset())) {
            destination = String.format("%s/%s", destination, getDataset());
        }

        setDestination(destination);
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
