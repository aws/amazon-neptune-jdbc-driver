/*
 * Copyright <2023> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.gremlin.GremlinConnection;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.neptune.gremlin.sql.SqlGremlinConnection;
import software.aws.neptune.jdbc.Driver;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import software.aws.neptune.opencypher.OpenCypherConnection;
import software.aws.neptune.opencypher.OpenCypherConnectionProperties;
import software.aws.neptune.sparql.SparqlConnection;
import software.aws.neptune.sparql.SparqlConnectionProperties;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class NeptuneDriver extends Driver implements java.sql.Driver {
    public static final String CONN_STRING_PREFIX = "jdbc:neptune:";
    private static final Logger LOGGER = LoggerFactory.getLogger(NeptuneDriver.class);
    private static final Pattern CONN_STRING_PATTERN = Pattern.compile(CONN_STRING_PREFIX + "(\\w+)://(.*)");

    static {
        try {
            DriverManager.registerDriver(new NeptuneDriver());
        } catch (final SQLException e) {
            LOGGER.error("Error registering driver: " + e.getMessage());
        }
    }

    private static final List<String> LANGUAGE_LIST = ImmutableList.of(
            "opencypher", "gremlin", "sqlgremlin", "sparql");

    @Override
    public boolean acceptsURL(final String url) throws SQLException {
        try {
            // TODO AN-550: Switch to map with class that holds Conn properties, key, query executor, etc.
            return url != null
                    && url.startsWith(CONN_STRING_PREFIX)
                    && LANGUAGE_LIST.contains(getLanguage(url, CONN_STRING_PATTERN));
        } catch (final SQLException ignored) {
        }
        return false;
    }

    @Override
    public java.sql.Connection connect(final String url, final Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        try {
            final DriverConnectionFactory connectionFactory = new DriverConnectionFactory();
            return connectionFactory.getConnection(url, info);
        } catch (final Exception e) {
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            LOGGER.error("Unexpected error while creating connection:", e);
            return null;
        }
    }

    private class DriverConnectionFactory {
        private final Pattern connStringPattern = Pattern.compile(CONN_STRING_PREFIX + "(\\w+)://(.*)");
        private static final String OPENCYPHER = "opencypher";
        private static final String GREMLIN = "gremlin";
        private static final String SQL_GREMLIN = "sqlgremlin";
        private static final String SPARQL = "sparql";

        public java.sql.Connection getConnection(final String url, final Properties info) throws SQLException {
            final String language = getLanguage(url, connStringPattern);
            final String propertyString = getPropertyString(url, connStringPattern);
            final String firstPropertyKey = getFirstPropertyKey(language);
            final Properties properties = createProperties(propertyString, firstPropertyKey, info);

            switch (language.toLowerCase()) {
                case OPENCYPHER:
                    return new OpenCypherConnection(new OpenCypherConnectionProperties(properties));
                case GREMLIN:
                    return new GremlinConnection(new GremlinConnectionProperties(properties));
                case SQL_GREMLIN:
                    return new SqlGremlinConnection(new GremlinConnectionProperties(properties));
                case SPARQL:
                    return new SparqlConnection(new SparqlConnectionProperties(properties));
                default:
                    LOGGER.error("Encountered Neptune JDBC connection string but failed to parse driver language.");
                    throw SqlError.createSQLException(
                            LOGGER, SqlState.CONNECTION_EXCEPTION,
                            SqlError.INVALID_CONNECTION_PROPERTY);
            }
        }

        private Properties createProperties(final String propertyString, final String firstPropertyKey,
                                         final Properties info) {
            final Properties properties =
                    parsePropertyString(propertyString, firstPropertyKey);
            if (info != null) {
                properties.putAll(info);
            }
            return properties;
        }

        private String getFirstPropertyKey(final String language) throws SQLException {
            if (OPENCYPHER.equalsIgnoreCase(language)) {
                return OpenCypherConnectionProperties.ENDPOINT_KEY;
            }
            if (GREMLIN.equalsIgnoreCase(language) || SQL_GREMLIN.equalsIgnoreCase(language)) {
                return GremlinConnectionProperties.CONTACT_POINT_KEY;
            }
            if (SPARQL.equalsIgnoreCase(language)) {
                return SparqlConnectionProperties.ENDPOINT_KEY;
            }
            LOGGER.error("Encountered Neptune JDBC connection string but failed to parse endpoint property.");
            throw SqlError.createSQLException(
                    LOGGER, SqlState.CONNECTION_EXCEPTION,
                    SqlError.INVALID_CONNECTION_PROPERTY);
        }
    }
}
