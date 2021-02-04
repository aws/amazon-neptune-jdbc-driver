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

package software.amazon.neptune;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.opencypher.OpenCypherConnection;
import software.amazon.neptune.opencypher.OpenCypherQueryExecutor;

import javax.annotation.Nullable;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class NeptuneDriver extends Driver implements java.sql.Driver {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherQueryExecutor.class);
    private static final String CONN_STRING_PREFIX = "jdbc:neptune:";
    private static final Pattern CONN_STRING_PATTERN = Pattern.compile(CONN_STRING_PREFIX + "(\\w+)://(.*)");

    static {
        try {
            DriverManager.registerDriver(new NeptuneDriver());
        } catch (SQLException e) {
            LOGGER.error("Error registering driver: " + e.getMessage());
        }
    }

    private final Map<String, Class<?>> connectionMap = ImmutableMap.of("opencypher", OpenCypherConnection.class);

    @Override
    public boolean acceptsURL(final @Nullable String url) throws SQLException {
        try {
            return url != null
                    && url.startsWith(CONN_STRING_PREFIX)
                    && connectionMap.containsKey(getLanguage(url, CONN_STRING_PATTERN));
        } catch (final SQLException ignored) {
        }
        return false;
    }

    @Override
    public java.sql.Connection connect(final @Nullable String url, final Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            LOGGER.error("Invalid url: {}.", url);
            return null;
        }
        final java.sql.Connection connection;
        final ConnectionProperties connectionProperties;
        try {
            final String language = getLanguage(url, CONN_STRING_PATTERN);
            final String propertyString = getPropertyString(url, CONN_STRING_PATTERN);
            final Properties properties = parsePropertyString(propertyString);
            properties.putAll(info);
            connectionProperties = new ConnectionProperties(properties);
            connection = (java.sql.Connection) connectionMap.get(language)
                    .getConstructor(ConnectionProperties.class)
                    .newInstance(connectionProperties);
        } catch (final Exception e) {
            LOGGER.error("Unexpected error while creating connection:", e);
            return null;
        }

        final int retryCount = connectionProperties.getConnectionRetryCount();
        for (int i = 0; i <= retryCount; i++) {
            if (connection.isValid(connectionProperties.getConnectionTimeout())) {
                return connection;
            }
        }
        LOGGER.error("Failed to create connection after {} attempts.", retryCount);
        return null;
    }
}
