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
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.opencypher.OpenCypherConnection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class NeptuneDriver extends Driver implements java.sql.Driver {
    private static final Pattern JDBC_PATTERN = Pattern.compile("jdbc:neptune:(\\w+)://(.*)");

    private final Map<String, Class<?>> connectionMap = ImmutableMap.of("opencypher", OpenCypherConnection.class);

    @Override
    public boolean acceptsURL(final String url) throws SQLException {
        try {
            return connectionMap.containsKey(getLanguage(url, JDBC_PATTERN));
        } catch (final SQLException ignored) {
        }
        return false;
    }

    @Override
    public java.sql.Connection connect(final String url, final Properties info) throws SQLException {
        final String language;
        final ConnectionProperties connectionProperties;
        try {
            language = getLanguage(url, JDBC_PATTERN);
            if (!connectionMap.containsKey(language)) {
                return null;
            }
            final String propertyString = getPropertyString(url, JDBC_PATTERN);
            final Properties properties = parsePropertyString(propertyString);
            properties.putAll(info);
            connectionProperties = new ConnectionProperties(properties);
        } catch (final Exception e) {
            return null;
        }

        java.sql.Connection connection = null;
        final int timeout = connectionProperties.getConnectionTimeout();
        final int retryCount = connectionProperties.getConnectionRetryCount();
        int count = 0;
        while (connection == null && count++ <= retryCount) { // retryCount could be zero
            try {
                connection = (java.sql.Connection) connectionMap
                    .get(language)
                    .getConstructor(ConnectionProperties.class)
                    .newInstance(connectionProperties);
                if (!connection.isValid(timeout)) {
                    connection = null;
                }
            } catch (final Exception ignore) {
            }
        }

        return connection;
    }
}
