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

package software.amazon.neptune.gremlin;

import com.google.common.collect.ImmutableList;
import io.netty.handler.ssl.SslContext;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.driver.LoadBalancingStrategy;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Gremlin connection properties class.
 */
public class GremlinConnectionProperties extends ConnectionProperties {
    public static final String CONTACT_POINT_KEY = "contactPoint";
    public static final String PATH_KEY = "path";
    public static final String PORT_KEY = "port";
    public static final String SERIALIZER_KEY = "serializer";
    public static final String ENABLE_SSL_KEY = "enableSsl";
    public static final String SSL_CONTEXT_KEY = "sslContext";
    public static final String SSL_ENABLED_PROTOCOLS_KEY = "sslEnabledProtocols";
    public static final String SSL_CIPHER_SUITES_KEY = "sslCipherSuites";
    public static final String SSL_SKIP_VALIDATION_KEY = "sslSkipCertValidation";
    public static final String KEY_STORE_KEY = "keyStore";
    public static final String KEY_STORE_PASSWORD_KEY = "keyStorePassword";
    public static final String KEY_STORE_TYPE_KEY = "keyStoreType";
    public static final String TRUST_STORE_KEY = "trustStore";
    public static final String TRUST_STORE_PASSWORD_KEY = "trustStorePassword";
    public static final String TRUST_STORE_TYPE_KEY = "trustStoreType";
    public static final String NIO_POOL_SIZE_KEY = "nioPoolSize";
    public static final String WORKER_POOL_SIZE_KEY = "workerPoolSize";
    public static final String MAX_CONNECTION_POOL_SIZE_KEY = "maxConnectionPoolSize";
    public static final String MIN_CONNECTION_POOL_SIZE_KEY = "minConnectionPoolSize";
    public static final String MAX_IN_PROCESS_PER_CONNECTION_KEY = "maxInProcessPerConnection";
    public static final String MIN_IN_PROCESS_PER_CONNECTION_KEY = "minInProcessPerConnection";
    public static final String MAX_SIMULT_USAGE_PER_CONNECTION_KEY = "maxSimultaneousUsagePerConnection";
    public static final String MIN_SIMULT_USAGE_PER_CONNECTION_KEY = "minSimultaneousUsagePerConnection";
    public static final String CHANNELIZER_KEY = "channelizer";
    public static final String KEEPALIVE_INTERVAL_KEY = "keepAliveInterval";
    public static final String RESULT_ITERATION_BATCH_SIZE_KEY = "resultIterationBatchSize";
    public static final String MAX_WAIT_FOR_CONNECTION_KEY = "maxWaitForConnection";
    public static final String MAX_WAIT_FOR_CLOSE_KEY = "maxWaitForClose";
    public static final String MAX_CONTENT_LENGTH_KEY = "maxContentLength";
    public static final String VALIDATION_REQUEST_KEY = "validationRequest";
    public static final String RECONNECT_INTERVAL_KEY = "reconnectInterval";
    public static final String LOAD_BALANCING_STRATEGY_KEY = "loadBalancingStrategy";

    private static final List<String> SUPPORTED_PROPERTIES_LIST = ImmutableList.<String>builder()
            .add(CONTACT_POINT_KEY)
            .add(PATH_KEY)
            .add(PORT_KEY)
            .add(SERIALIZER_KEY)
            .add(ENABLE_SSL_KEY)
            .add(SSL_CONTEXT_KEY)
            .add(SSL_ENABLED_PROTOCOLS_KEY)
            .add(SSL_CIPHER_SUITES_KEY)
            .add(SSL_SKIP_VALIDATION_KEY)
            .add(KEY_STORE_KEY)
            .add(KEY_STORE_PASSWORD_KEY)
            .add(KEY_STORE_TYPE_KEY)
            .add(TRUST_STORE_KEY)
            .add(TRUST_STORE_PASSWORD_KEY)
            .add(TRUST_STORE_TYPE_KEY)
            .add(NIO_POOL_SIZE_KEY)
            .add(WORKER_POOL_SIZE_KEY)
            .add(MAX_CONNECTION_POOL_SIZE_KEY)
            .add(MIN_CONNECTION_POOL_SIZE_KEY)
            .add(MAX_IN_PROCESS_PER_CONNECTION_KEY)
            .add(MIN_IN_PROCESS_PER_CONNECTION_KEY)
            .add(MAX_SIMULT_USAGE_PER_CONNECTION_KEY)
            .add(MIN_SIMULT_USAGE_PER_CONNECTION_KEY)
            .add(CHANNELIZER_KEY)
            .add(KEEPALIVE_INTERVAL_KEY)
            .add(RESULT_ITERATION_BATCH_SIZE_KEY)
            .add(MAX_WAIT_FOR_CONNECTION_KEY)
            .add(MAX_WAIT_FOR_CLOSE_KEY)
            .add(MAX_CONTENT_LENGTH_KEY)
            .add(VALIDATION_REQUEST_KEY)
            .add(RECONNECT_INTERVAL_KEY)
            .add(LOAD_BALANCING_STRATEGY_KEY)
            .build();

    public static final String DEFAULT_PATH = "/gremlin";
    public static final int DEFAULT_PORT = 8182;
    public static final boolean DEFAULT_ENABLE_SSL = true;
    public static final boolean DEFAULT_SSL_SKIP_VALIDATION = false;

    public static final Map<String, Object> DEFAULT_PROPERTIES_MAP = new HashMap<>();
    private static final Map<String, ConnectionProperties.PropertyConverter<?>> PROPERTY_CONVERTER_MAP = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinConnectionProperties.class);

    static {
        PROPERTY_CONVERTER_MAP.put(CONTACT_POINT_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(PATH_KEY, (key, value) -> value);
        PROPERTY_CONVERTER_MAP.put(PORT_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(ENABLE_SSL_KEY, ConnectionProperties::toBoolean);
        PROPERTY_CONVERTER_MAP.put(NIO_POOL_SIZE_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(WORKER_POOL_SIZE_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(MAX_CONNECTION_POOL_SIZE_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(MIN_CONNECTION_POOL_SIZE_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(MAX_IN_PROCESS_PER_CONNECTION_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(MIN_IN_PROCESS_PER_CONNECTION_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(MAX_SIMULT_USAGE_PER_CONNECTION_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(MIN_SIMULT_USAGE_PER_CONNECTION_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(KEEPALIVE_INTERVAL_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(RESULT_ITERATION_BATCH_SIZE_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(MAX_WAIT_FOR_CONNECTION_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(MAX_WAIT_FOR_CLOSE_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(MAX_CONTENT_LENGTH_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(RECONNECT_INTERVAL_KEY, ConnectionProperties::toUnsigned);
        PROPERTY_CONVERTER_MAP.put(SSL_SKIP_VALIDATION_KEY, ConnectionProperties::toBoolean);
    }

    static {
        DEFAULT_PROPERTIES_MAP.put(CONTACT_POINT_KEY, "");
        DEFAULT_PROPERTIES_MAP.put(PATH_KEY, DEFAULT_PATH);
        DEFAULT_PROPERTIES_MAP.put(PORT_KEY, DEFAULT_PORT);
        DEFAULT_PROPERTIES_MAP.put(ENABLE_SSL_KEY, DEFAULT_ENABLE_SSL);
        DEFAULT_PROPERTIES_MAP.put(SSL_SKIP_VALIDATION_KEY, DEFAULT_SSL_SKIP_VALIDATION);
    }

    /**
     * GremlinConnectionProperties constructor.
     */
    public GremlinConnectionProperties() throws SQLException {
        super(new Properties(), DEFAULT_PROPERTIES_MAP, PROPERTY_CONVERTER_MAP);
    }

    /**
     * GremlinConnectionProperties constructor.
     * @param properties Properties to examine and extract key details from.
     */
    public GremlinConnectionProperties(final Properties properties) throws SQLException {
        super(properties, DEFAULT_PROPERTIES_MAP, PROPERTY_CONVERTER_MAP);
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
     * Gets the path to the Gremlin service on the host.
     *
     * @return The path to the Gremlin service.
     */
    public String getPath() {
        return getProperty(PATH_KEY);
    }

    /**
     * Sets the path to the Gremlin service on the host.
     *
     * @param path The path to the Gremlin service.
     * @throws SQLException if value is invalid.
     */
    public void setPath(@NonNull final String path) throws SQLException {
        setProperty(PATH_KEY,
                (String) PROPERTY_CONVERTER_MAP.get(PATH_KEY).convert(PATH_KEY, path));
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
     * Check whether the Serializer is an object.
     *
     * @return True if Serializer is an object, otherwise false.
     */
    public boolean isSerializerObject() {
        if (!containsKey(SERIALIZER_KEY)) {
            return false;
        }
        return (get(SERIALIZER_KEY) instanceof MessageSerializer);
    }

    /**
     * Check whether the Serializer is a string.
     *
     * @return True if Serializer is a string, otherwise false.
     */
    public boolean isSerializerString() {
        if (!containsKey(SERIALIZER_KEY)) {
            return false;
        }
        return (get(SERIALIZER_KEY) instanceof String);
    }

    /**
     * Gets the Serializer object to use.
     *
     * @return The Serializer object.
     */
    public MessageSerializer getSerializerObject() {
        if (!containsKey(SERIALIZER_KEY)) {
            return null;
        }
        return (MessageSerializer) get(SERIALIZER_KEY);
    }

    /**
     * Gets the Serializer class name.
     *
     * @return The Serializer class name.
     */
    public String getSerializerString() {
        if (!containsKey(SERIALIZER_KEY)) {
            return null;
        }
        return (String) get(SERIALIZER_KEY);
    }

    /**
     * Sets the MessageSerializer object to use.
     *
     * @param serializer The MessageSerializer object.
     * @throws SQLException if value is invalid.
     */
    public void setSerializer(@NonNull final MessageSerializer serializer) throws SQLException {
        put(SERIALIZER_KEY, serializer);
    }

    /**
     * Sets the Serializer to use given the exact name of a Serializers enum.
     *
     * @param serializerMimeType The exact name of a Serializers enum.
     * @throws SQLException if value is invalid.
     */
    public void setSerializer(@NonNull final String serializerMimeType) throws SQLException {
        put(SERIALIZER_KEY, serializerMimeType);
    }

    /**
     * Gets the enable ssl flag.
     *
     * @return The enable ssl flag.
     */
    public boolean getEnableSsl() {
        return (boolean) get(ENABLE_SSL_KEY);
    }

    /**
     * Sets the enable ssl flag.
     *
     * @param enableSsl The enable ssl flag.
     */
    public void setEnableSsl(final boolean enableSsl) {
        put(ENABLE_SSL_KEY, enableSsl);
    }

    /**
     * Gets the SslContext.
     *
     * @return The SslContext.
     */
    public SslContext getSslContext() {
        if (!containsKey(SSL_CONTEXT_KEY)) {
            return null;
        }
        return (SslContext) get(SSL_CONTEXT_KEY);
    }

    /**
     * Sets the SslContext.
     *
     * @param sslContext The SslContext.
     * @throws SQLException if value is invalid.
     */
    public void setSslContext(@NonNull final SslContext sslContext) throws SQLException {
        put(SSL_CONTEXT_KEY, sslContext);
    }

    /**
     * Gets the list of enabled SSL protocols.
     *
     * @return The list of enabled SSL protocols.
     */
    public List<?> getSslEnabledProtocols() {
        if (!containsKey(SSL_ENABLED_PROTOCOLS_KEY)) {
            return null;
        }
        return (List<?>) get(SSL_ENABLED_PROTOCOLS_KEY);
    }

    /**
     * Sets the list of SSL protocols to enable.
     *
     * @param sslEnabledProtocols The list of enabled SSL protocols.
     * @throws SQLException if value is invalid.
     */
    public void setSslEnabledProtocols(@NonNull final List<String> sslEnabledProtocols) throws SQLException {
        put(SSL_ENABLED_PROTOCOLS_KEY, sslEnabledProtocols);
    }

    /**
     * Gets the list of enabled cipher suites.
     *
     * @return The list of enabled cipher suites.
     */
    public List<?> getSslCipherSuites() {
        if (!containsKey(SSL_CIPHER_SUITES_KEY)) {
            return null;
        }
        return (List<?>) get(SSL_CIPHER_SUITES_KEY);
    }

    /**
     * Sets the list of cipher suites to enable.
     *
     * @param sslCipherSuites The list of enabled cipher suites.
     * @throws SQLException if value is invalid.
     */
    public void setSslCipherSuites(@NonNull final List<String> sslCipherSuites) throws SQLException {
        put(SSL_CIPHER_SUITES_KEY, sslCipherSuites);
    }

    /**
     * Gets whether to trust all certificates and not perform any validation.
     *
     * @return The skip SSL validation flag.
     */
    public boolean getSslSkipCertValidation() {
        return (boolean) get(SSL_SKIP_VALIDATION_KEY);
    }

    /**
     * Sets whether to trust all certificates and not perform any validation.
     *
     * @param sslSkipCertValidation The skip SSL validation flag.
     */
    public void setSslSkipCertValidation(final boolean sslSkipCertValidation) {
        put(SSL_SKIP_VALIDATION_KEY, sslSkipCertValidation);
    }

    /**
     * Gets the file location of the private key in JKS or PKCS#12 format.
     *
     * @return The file location of the private key store, or null if not found.
     */
    public String getKeyStore() {
        if (!containsKey(KEY_STORE_KEY)) {
            return null;
        }
        return getProperty(KEY_STORE_KEY);
    }

    /**
     * Sets the file location of the private key in JKS or PKCS#12 format.
     *
     * @param keyStore The file location of the private key store.
     * @throws SQLException if value is invalid.
     */
    public void setKeyStore(@NonNull final String keyStore) throws SQLException {
        put(KEY_STORE_KEY, keyStore);
    }

    /**
     * Gets the password of the keyStore, or null if it's not password-protected.
     *
     * @return The password of the keyStore, or null if it's not password-protected.
     */
    public String getKeyStorePassword() {
        if (!containsKey(KEY_STORE_PASSWORD_KEY)) {
            return null;
        }
        return getProperty(KEY_STORE_PASSWORD_KEY);
    }

    /**
     * Sets the password of the keyStore, or null if it's not password-protected.
     *
     * @param keyStorePassword The password of the keyStore.
     * @throws SQLException if value is invalid.
     */
    public void setKeyStorePassword(final String keyStorePassword) throws SQLException {
        if (keyStorePassword != null) {
            put(KEY_STORE_PASSWORD_KEY, keyStorePassword);
        } else  {
            remove(KEY_STORE_PASSWORD_KEY);
        }
    }

    /**
     * Gets the format of the keyStore, either JKS or PKCS12.
     *
     * @return The format of the keyStore, or null if not found.
     */
    public String getKeyStoreType() {
        if (!containsKey(KEY_STORE_TYPE_KEY)) {
            return null;
        }
        return getProperty(KEY_STORE_TYPE_KEY);
    }

    /**
     * Sets the format of the keyStore, either JKS or PKCS12.
     *
     * @param keyStoreType TThe format of the keyStore.
     * @throws SQLException if value is invalid.
     */
    public void setKeyStoreType(@NonNull final String keyStoreType) throws SQLException {
        put(KEY_STORE_TYPE_KEY, keyStoreType);
    }

    /**
     * Gets the file location for a SSL Certificate Chain to use when SSL is enabled.
     *
     * @return The file location for a SSL Certificate Chain.
     */
    public String getTrustStore() {
        if (!containsKey(TRUST_STORE_KEY)) {
            return null;
        }
        return getProperty(TRUST_STORE_KEY);
    }

    /**
     * Sets the file location for a SSL Certificate Chain to use when SSL is enabled.
     *
     * @param trustStore The file location for a SSL Certificate Chain.
     * @throws SQLException if value is invalid.
     */
    public void setTrustStore(@NonNull final String trustStore) throws SQLException {
        put(TRUST_STORE_KEY, trustStore);
    }

    /**
     * Gets the password of the trustStore, or null if it's not password-protected.
     *
     * @return The password of the trustStore, or null if it's not password-protected.
     */
    public String getTrustStorePassword() {
        if (!containsKey(TRUST_STORE_PASSWORD_KEY)) {
            return null;
        }
        return getProperty(TRUST_STORE_PASSWORD_KEY);
    }

    /**
     * Sets the password of the trustStore, or null if it's not password-protected.
     *
     * @param trustStorePassword The password of the trustStore.
     * @throws SQLException if value is invalid.
     */
    public void setTrustStorePassword(final String trustStorePassword) throws SQLException {
        if (trustStorePassword != null) {
            put(TRUST_STORE_PASSWORD_KEY, trustStorePassword);
        } else  {
            remove(TRUST_STORE_PASSWORD_KEY);
        }
    }

    /**
     * Gets the format of the trustStore, either JKS or PKCS12.
     *
     * @return The format of the trustStore, or null if not found.
     */
    public String getTrustStoreType() {
        if (!containsKey(TRUST_STORE_TYPE_KEY)) {
            return null;
        }
        return getProperty(TRUST_STORE_TYPE_KEY);
    }

    /**
     * Sets the format of the trustStore, either JKS or PKCS12.
     *
     * @param trustStoreType TThe format of the trustStore.
     * @throws SQLException if value is invalid.
     */
    public void setTrustStoreType(@NonNull final String trustStoreType) throws SQLException {
        put(TRUST_STORE_TYPE_KEY, trustStoreType);
    }

    /**
     * Gets the size of the pool for handling request/response operations.
     *
     * @return The size of the Nio pool.
     */
    public int getNioPoolSize() {
        if (!containsKey(NIO_POOL_SIZE_KEY)) {
            return 0;
        }
        return (int) get(NIO_POOL_SIZE_KEY);
    }

    /**
     * Sets the size of the pool for handling request/response operations.
     *
     * @param nioPoolSize The size of the Nio pool.
     * @throws SQLException if value is invalid.
     */
    public void setNioPoolSize(final int nioPoolSize) throws SQLException {
        if (nioPoolSize < 0) {
            throw invalidConnectionPropertyError(NIO_POOL_SIZE_KEY, nioPoolSize);
        }
        put(NIO_POOL_SIZE_KEY, nioPoolSize);
    }

    /**
     * Gets the size of the pool for handling background work.
     *
     * @return The size of the worker pool.
     */
    public int getWorkerPoolSize() {
        if (!containsKey(WORKER_POOL_SIZE_KEY)) {
            return 0;
        }
        return (int) get(WORKER_POOL_SIZE_KEY);
    }

    /**
     * Sets the size of the pool for handling background work.
     *
     * @param workerPoolSize The size of the worker pool.
     * @throws SQLException if value is invalid.
     */
    public void setWorkerPoolSize(final int workerPoolSize) throws SQLException {
        if (workerPoolSize < 0) {
            throw invalidConnectionPropertyError(WORKER_POOL_SIZE_KEY, workerPoolSize);
        }
        put(WORKER_POOL_SIZE_KEY, workerPoolSize);
    }

    /**
     * Gets the maximum connection pool size.
     *
     * @return The maximum connection pool size.
     */
    public int getMaxConnectionPoolSize() {
        if (!containsKey(MAX_CONNECTION_POOL_SIZE_KEY)) {
            return 0;
        }
        return (int) get(MAX_CONNECTION_POOL_SIZE_KEY);
    }

    /**
     * Sets the maximum connection pool size.
     *
     * @param maxConnectionPoolSize The maximum connection pool size.
     * @throws SQLException if value is invalid.
     */
    public void setMaxConnectionPoolSize(final int maxConnectionPoolSize) throws SQLException {
        if (maxConnectionPoolSize < 0) {
            throw invalidConnectionPropertyError(MAX_CONNECTION_POOL_SIZE_KEY, maxConnectionPoolSize);
        }
        put(MAX_CONNECTION_POOL_SIZE_KEY, maxConnectionPoolSize);
    }

    /**
     * Gets the minimum connection pool size.
     *
     * @return The minimum connection pool size.
     */
    public int getMinConnectionPoolSize() {
        if (!containsKey(MIN_CONNECTION_POOL_SIZE_KEY)) {
            return 0;
        }
        return (int) get(MIN_CONNECTION_POOL_SIZE_KEY);
    }

    /**
     * Sets the minimum connection pool size.
     *
     * @param minConnectionPoolSize The minimum connection pool size.
     * @throws SQLException if value is invalid.
     */
    public void setMinConnectionPoolSize(final int minConnectionPoolSize) throws SQLException {
        if (minConnectionPoolSize < 0) {
            throw invalidConnectionPropertyError(MIN_CONNECTION_POOL_SIZE_KEY, minConnectionPoolSize);
        }
        put(MIN_CONNECTION_POOL_SIZE_KEY, minConnectionPoolSize);
    }

    /**
     * Gets the maximum number of in-flight requests that can occur on a Connection.
     *
     * @return The maximum in-flight requests per Connection.
     */
    public int getMaxInProcessPerConnection() {
        if (!containsKey(MAX_IN_PROCESS_PER_CONNECTION_KEY)) {
            return 0;
        }
        return (int) get(MAX_IN_PROCESS_PER_CONNECTION_KEY);
    }

    /**
     * Sets the maximum number of in-flight requests that can occur on a Connection.
     *
     * @param maxInProcessPerConnection The maximum in-flight requests per Connection.
     * @throws SQLException if value is invalid.
     */
    public void setMaxInProcessPerConnection(final int maxInProcessPerConnection) throws SQLException {
        if (maxInProcessPerConnection < 0) {
            throw invalidConnectionPropertyError(MAX_IN_PROCESS_PER_CONNECTION_KEY, maxInProcessPerConnection);
        }
        put(MAX_IN_PROCESS_PER_CONNECTION_KEY, maxInProcessPerConnection);
    }

    /**
     * Gets the minimum number of in-flight requests that can occur on a Connection before it is considered for closing on return to the ConnectionPool.
     *
     * @return The minimum in-flight requests per Connection.
     */
    public int getMinInProcessPerConnection() {
        if (!containsKey(MIN_IN_PROCESS_PER_CONNECTION_KEY)) {
            return 0;
        }
        return (int) get(MIN_IN_PROCESS_PER_CONNECTION_KEY);
    }

    /**
     * Sets the minimum number of in-flight requests that can occur on a Connection before it is considered for closing on return to the ConnectionPool.
     *
     * @param minInProcessPerConnection The minimum in-flight requests per Connection.
     * @throws SQLException if value is invalid.
     */
    public void setMinInProcessPerConnection(final int minInProcessPerConnection) throws SQLException {
        if (minInProcessPerConnection < 0) {
            throw invalidConnectionPropertyError(MIN_IN_PROCESS_PER_CONNECTION_KEY, minInProcessPerConnection);
        }
        put(MIN_IN_PROCESS_PER_CONNECTION_KEY, minInProcessPerConnection);
    }

    /**
     * Gets the maximum number of times that a Connection can be borrowed from the pool simultaneously.
     *
     * @return The maximum number of simultaneous usage per Connection.
     */
    public int getMaxSimultaneousUsagePerConnection() {
        if (!containsKey(MAX_SIMULT_USAGE_PER_CONNECTION_KEY)) {
            return 0;
        }
        return (int) get(MAX_SIMULT_USAGE_PER_CONNECTION_KEY);
    }

    /**
     * Sets the maximum number of times that a Connection can be borrowed from the pool simultaneously.
     *
     * @param maxSimultUsagePerConnection The maximum number of simultaneous usage per Connection.
     * @throws SQLException if value is invalid.
     */
    public void setMaxSimultaneousUsagePerConnection(final int maxSimultUsagePerConnection) throws SQLException {
        if (maxSimultUsagePerConnection < 0) {
            throw invalidConnectionPropertyError(MAX_SIMULT_USAGE_PER_CONNECTION_KEY, maxSimultUsagePerConnection);
        }
        put(MAX_SIMULT_USAGE_PER_CONNECTION_KEY, maxSimultUsagePerConnection);
    }

    /**
     * Gets the minimum number of times that a Connection should be borrowed from the pool before it falls under consideration for closing.
     *
     * @return The minimum number of simultaneous usage per Connection.
     */
    public int getMinSimultaneousUsagePerConnection() {
        if (!containsKey(MIN_SIMULT_USAGE_PER_CONNECTION_KEY)) {
            return 0;
        }
        return (int) get(MIN_SIMULT_USAGE_PER_CONNECTION_KEY);
    }

    /**
     * Sets the minimum number of times that a Connection should be borrowed from the pool before it falls under consideration for closing.
     *
     * @param minSimultUsagePerConnection The minimum number of simultaneous usage per Connection.
     * @throws SQLException if value is invalid.
     */
    public void setMinSimultaneousUsagePerConnection(final int minSimultUsagePerConnection) throws SQLException {
        if (minSimultUsagePerConnection < 0) {
            throw invalidConnectionPropertyError(MIN_SIMULT_USAGE_PER_CONNECTION_KEY, minSimultUsagePerConnection);
        }
        put(MIN_SIMULT_USAGE_PER_CONNECTION_KEY, minSimultUsagePerConnection);
    }

    /**
     * Check whether the Channelizer is a class.
     *
     * @return True if Channelizer is a class, otherwise false.
     */
    public boolean isChannelizerGeneric() {
        if (!containsKey(CHANNELIZER_KEY)) {
            return false;
        }
        return (get(CHANNELIZER_KEY) instanceof Class<?>);
    }

    /**
     * Check whether the Channelizer is a string.
     *
     * @return True if Channelizer is a string, otherwise false.
     */
    public boolean isChannelizerString() {
        if (!containsKey(CHANNELIZER_KEY)) {
            return false;
        }
        return (get(CHANNELIZER_KEY) instanceof String);
    }

    /**
     * Gets the Channelizer class.
     *
     * @return The Channelizer class.
     */
    public Class<?> getChannelizerGeneric() {
        if (!containsKey(CHANNELIZER_KEY)) {
            return null;
        }
        return (Class<?>) get(CHANNELIZER_KEY);
    }

    /**
     * Gets the Channelizer class name.
     *
     * @return The Channelizer class name.
     */
    public String getChannelizerString() {
        if (!containsKey(CHANNELIZER_KEY)) {
            return null;
        }
        return (String) get(CHANNELIZER_KEY);
    }

    /**
     * Sets the Channelizer implementation to use on the client when creating a Connection.
     *
     * @param channelizerClass The Channelizer class.
     * @throws SQLException if value is invalid.
     */
    public void setChannelizer(@NonNull final Class<?> channelizerClass) throws SQLException {
        put(CHANNELIZER_KEY, channelizerClass);
    }

    /**
     * Sets the Channelizer implementation to use on the client when creating a Connection.
     *
     * @param channelizerClass The Channelizer class name.
     * @throws SQLException if value is invalid.
     */
    public void setChannelizer(@NonNull final String channelizerClass) throws SQLException {
        put(CHANNELIZER_KEY, channelizerClass);
    }

    /**
     * Gets the keep alive interval.
     *
     * @return The keep alive interval.
     */
    public int getKeepAliveInterval() {
        if (!containsKey(KEEPALIVE_INTERVAL_KEY)) {
            return 0;
        }
        return (int) get(KEEPALIVE_INTERVAL_KEY);
    }

    /**
     * Sets the keep alive interval, as the length of time in milliseconds to wait on an idle connection
     * before sending a keep-alive request. This setting is only relevant to Channelizer implementations
     * that return true for Channelizer.supportsKeepAlive(). Set to zero to disable this feature.
     *
     * @param keepAliveInterval The keep alive interval.
     */
    public void setKeepAliveInterval(final int keepAliveInterval) throws SQLException {
        if (keepAliveInterval < 0) {
            throw invalidConnectionPropertyError(KEEPALIVE_INTERVAL_KEY, keepAliveInterval);
        }
        put(KEEPALIVE_INTERVAL_KEY, keepAliveInterval);
    }

    /**
     * Gets how many results are returned per batch.
     *
     * @return The result iteration batch size.
     */
    public int getResultIterationBatchSize() {
        if (!containsKey(RESULT_ITERATION_BATCH_SIZE_KEY)) {
            return 0;
        }
        return (int) get(RESULT_ITERATION_BATCH_SIZE_KEY);
    }

    /**
     * Sets how many results are returned per batch.
     *
     * @param resultIterationBatchSize The result iteration batch size.
     */
    public void setResultIterationBatchSize(final int resultIterationBatchSize) throws SQLException {
        if (resultIterationBatchSize < 0) {
            throw invalidConnectionPropertyError(RESULT_ITERATION_BATCH_SIZE_KEY, resultIterationBatchSize);
        }
        put(RESULT_ITERATION_BATCH_SIZE_KEY, resultIterationBatchSize);
    }

    /**
     * Gets the maximum amount of time to wait for a connection to be borrowed from the connection pool.
     *
     * @return The maximum wait for Connection.
     */
    public int getMaxWaitForConnection() {
        if (!containsKey(MAX_WAIT_FOR_CONNECTION_KEY)) {
            return 0;
        }
        return (int) get(MAX_WAIT_FOR_CONNECTION_KEY);
    }

    /**
     * Sets the maximum amount of time to wait for a connection to be borrowed from the connection pool.
     *
     * @param maxWaitForConnection The maximum wait for Connection.
     * @throws SQLException if value is invalid.
     */
    public void setMaxWaitForConnection(final int maxWaitForConnection) throws SQLException {
        if (maxWaitForConnection < 0) {
            throw invalidConnectionPropertyError(MAX_WAIT_FOR_CONNECTION_KEY, maxWaitForConnection);
        }
        put(MAX_WAIT_FOR_CONNECTION_KEY, maxWaitForConnection);
    }

    /**
     * Gets the maximum amount of time in milliseconds to wait for the Connection to close before timing out.
     *
     * @return The maximum wait to close.
     */
    public int getMaxWaitForClose() {
        if (!containsKey(MAX_WAIT_FOR_CLOSE_KEY)) {
            return 0;
        }
        return (int) get(MAX_WAIT_FOR_CLOSE_KEY);
    }

    /**
     * Sets the maximum amount of time in milliseconds to wait for the Connection to close before timing out.
     *
     * @param maxWaitForClose The maximum wait to close.
     * @throws SQLException if value is invalid.
     */
    public void setMaxWaitForClose(final int maxWaitForClose) throws SQLException {
        if (maxWaitForClose < 0) {
            throw invalidConnectionPropertyError(MAX_WAIT_FOR_CLOSE_KEY, maxWaitForClose);
        }
        put(MAX_WAIT_FOR_CLOSE_KEY, maxWaitForClose);
    }

    /**
     * Gets the maximum size in bytes of any request sent to the server.
     *
     * @return The maximum size in bytes.
     */
    public int getMaxContentLength() {
        if (!containsKey(MAX_CONTENT_LENGTH_KEY)) {
            return 0;
        }
        return (int) get(MAX_CONTENT_LENGTH_KEY);
    }

    /**
     * Sets the maximum size in bytes of any request sent to the server.
     *
     * @param maxContentLength The maximum size in bytes.
     * @throws SQLException if value is invalid.
     */
    public void setMaxContentLength(final int maxContentLength) throws SQLException {
        if (maxContentLength < 0) {
            throw invalidConnectionPropertyError(MAX_CONTENT_LENGTH_KEY, maxContentLength);
        }
        put(MAX_CONTENT_LENGTH_KEY, maxContentLength);
    }

    /**
     * Gets a valid Gremlin script that can be used to test remote operations.
     *
     * @return The Gremlin script.
     */
    public String getValidationRequest() {
        if (!containsKey(VALIDATION_REQUEST_KEY)) {
            return null;
        }
        return getProperty(VALIDATION_REQUEST_KEY);
    }

    /**
     * Sets a valid Gremlin script that can be used to test remote operations.
     *
     * @param script The Gremlin script.
     * @throws SQLException if value is invalid.
     */
    public void setValidationRequest(@NonNull final String script) throws SQLException {
        put(VALIDATION_REQUEST_KEY, script);
    }

    /**
     * Gets the time in milliseconds to wait between retries when attempting to reconnect to a dead host.
     *
     * @return The reconnect interval.
     */
    public int getReconnectInterval() {
        if (!containsKey(RECONNECT_INTERVAL_KEY)) {
            return 0;
        }
        return (int) get(RECONNECT_INTERVAL_KEY);
    }

    /**
     * Sets the time in milliseconds to wait between retries when attempting to reconnect to a dead host.
     *
     * @param reconnectInterval The reconnect interval.
     * @throws SQLException if value is invalid.
     */
    public void setReconnectInterval(final int reconnectInterval) throws SQLException {
        if (reconnectInterval < 0) {
            throw invalidConnectionPropertyError(RECONNECT_INTERVAL_KEY, reconnectInterval);
        }
        put(RECONNECT_INTERVAL_KEY, reconnectInterval);
    }

    /**
     * Gets the load balancing strategy.
     *
     * @return The load balancing strategy.
     */
    public LoadBalancingStrategy getLoadBalancingStrategy() {
        if (!containsKey(LOAD_BALANCING_STRATEGY_KEY)) {
            return null;
        }
        return (LoadBalancingStrategy) get(LOAD_BALANCING_STRATEGY_KEY);
    }

    /**
     * Sets the load balancing strategy to use on the client side.
     *
     * @param strategy The load balancing strategy.
     * @throws SQLException if value is invalid.
     */
    public void setLoadBalancingStrategy(@NonNull final LoadBalancingStrategy strategy) throws SQLException {
        put(LOAD_BALANCING_STRATEGY_KEY, strategy);
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
                throw missingConnectionPropertyError("A Region must be provided to use IAMSigV4 Authentication. Set the SERVICE_REGION environment variable to the appropriate region, such as 'us-east-1'.");
            }

            if (!getEnableSsl()) {
                throw invalidConnectionPropertyValueError(ENABLE_SSL_KEY,
                        "SSL encryption must be enabled if IAMSigV4 is used");
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
        return SUPPORTED_PROPERTIES_LIST.contains(name);
    }

    /**
     * Get the number of processors available to the Java virtual machine.
     * @return The number of processors available to the Java virtual machine.
     */
    private static int getNumberOfProcessors() {
        // get the runtime object associated with the current Java application
        final Runtime runtime = Runtime.getRuntime();

        return runtime.availableProcessors();
    }
}
