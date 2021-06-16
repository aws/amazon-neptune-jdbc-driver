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

package software.amazon.neptune;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.gremlin.GremlinConnection;
import software.amazon.neptune.gremlin.GremlinConnectionProperties;
import software.amazon.neptune.gremlin.sql.SqlGremlinConnection;
import software.amazon.neptune.opencypher.OpenCypherConnection;
import software.amazon.neptune.opencypher.OpenCypherConnectionProperties;
import javax.annotation.Nullable;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class NeptuneDriver extends Driver implements java.sql.Driver {
    public static final String CONN_STRING_PREFIX = "jdbc:neptune:";
    private static final Logger LOGGER = LoggerFactory.getLogger(NeptuneDriver.class);
    private static final Pattern CONN_STRING_PATTERN = Pattern.compile(CONN_STRING_PREFIX + "(\\w+)://(.*)");
    // TODO AN-550: Switch to map with class that holds Conn properties, key, query executor, etc.
    private static final String OPENCYPHER = "opencypher";
    private static final String GREMLIN = "gremlin";
    private static final String SQL_GREMLIN = "sqlgremlin";

    static {
        try {
            DriverManager.registerDriver(new NeptuneDriver());
        } catch (final SQLException e) {
            LOGGER.error("Error registering driver: " + e.getMessage());
        }
    }

    // TODO AN-550: Switch to map with class that holds Conn properties, key, query executor, etc.
    private final Map<String, Class<?>> connectionMap = ImmutableMap.of(
            "opencypher", OpenCypherConnection.class,
            "gremlin", GremlinConnection.class,
            "sqlgremlin", SqlGremlinConnection.class);

    @Override
    public boolean acceptsURL(final @Nullable String url) throws SQLException {
        try {
            // TODO AN-550: Switch to map with class that holds Conn properties, key, query executor, etc.
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
            return null;
        }

        try {
            final String language = getLanguage(url, CONN_STRING_PATTERN);
            final Properties properties =
                    parsePropertyString(getPropertyString(url, CONN_STRING_PATTERN), firstPropertyKey(language));
            if (info != null) {
                properties.putAll(info);
            }
            return (java.sql.Connection) connectionMap.get(language)
                    .getConstructor(ConnectionProperties.class)
                    .newInstance(connectionProperties(language, properties));
        } catch (final Exception e) {
            LOGGER.error("Unexpected error while creating connection:", e);
            return null;
        }
    }

    private ConnectionProperties connectionProperties(final String language, final Properties properties)
            throws SQLException {
        // TODO AN-550: Switch to map with class that holds Conn properties, key, query executor, etc.
        if (OPENCYPHER.equalsIgnoreCase(language)) {
            return new OpenCypherConnectionProperties(properties);
        }
        if (GREMLIN.equalsIgnoreCase(language) || SQL_GREMLIN.equalsIgnoreCase(language)) {
            return new GremlinConnectionProperties(properties);
        }
        // TODO - implement for other languages
        return new OpenCypherConnectionProperties(properties);
    }

    private String firstPropertyKey(final String language) {
        // TODO AN-550: Switch to map with class that holds Conn properties, key, query executor, etc.
        if (OPENCYPHER.equalsIgnoreCase(language)) {
            return OpenCypherConnectionProperties.ENDPOINT_KEY;
        }
        if (GREMLIN.equalsIgnoreCase(language) || SQL_GREMLIN.equalsIgnoreCase(language)) {
            return GremlinConnectionProperties.CONTACT_POINT_KEY;
        }
        // TODO - implement for other languages
        return OpenCypherConnectionProperties.ENDPOINT_KEY;
    }
}
