/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.gremlin;

import software.aws.neptune.gremlin.resultset.GremlinResultSet;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static software.aws.neptune.gremlin.GremlinConnectionProperties.CONTACT_POINT_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.ENABLE_SSL_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.MAX_CONTENT_LENGTH_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.PORT_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.SSL_SKIP_VALIDATION_KEY;

public class GremlinHelper {
    /**
     * Function to get properties for Gremlin connection.
     *
     * @param hostname hostname for properties.
     * @param port     port number for properties.
     * @return Properties for Gremlin connection.
     */
    public static Properties getProperties(final String hostname, final int port) {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        properties.put(CONTACT_POINT_KEY, hostname);
        properties.put(PORT_KEY, port);
        properties.put(ENABLE_SSL_KEY, false);
        properties.put(SSL_SKIP_VALIDATION_KEY, true);
        return properties;
    }

    /**
     * Function to get properties for Gremlin connection.
     *
     * @param hostname         hostname for properties.
     * @param port             port number for properties.
     * @param maxContentLength max content length for properties.
     * @return Properties for Gremlin connection.
     */
    public static Properties getProperties(final String hostname, final int port, final int maxContentLength) {
        final Properties properties = getProperties(hostname, port);
        properties.put(MAX_CONTENT_LENGTH_KEY, maxContentLength);
        return properties;
    }

    /**
     * Function to construct Gremlin query that creates vertex.
     *
     * @param label      Vertex label.
     * @param properties Map containing Vertex properties.
     * @return Gremlin query that creates vertex.
     */
    public static String createVertexQuery(final String label, final Map<String, ?> properties) {
        final String q = "\"";
        final StringBuilder sb = new StringBuilder();
        sb.append("g.addV(").append(q).append(label).append(q).append(")");
        for (final Map.Entry<String, ?> entry : properties.entrySet()) {
            sb.append(".property(")
                    .append(q).append(entry.getKey()).append(q)
                    .append(", ");
            if (entry.getValue() instanceof String) {
                sb.append(q).append(entry.getValue()).append(q);
            } else {
                if (entry.getValue() instanceof Float) {
                    sb.append(((Float)entry.getValue()).floatValue());
                } else if (entry.getValue() instanceof Double) {
                    sb.append(((Double)entry.getValue()).doubleValue());
                } else {
                    sb.append(entry.getValue());
                }
            }
            sb.append(")");
        }
        System.out.println("Query: " + sb.toString());
        return sb.toString();
    }

    /**
     * Function to construct Gremlin query that gets vertex.
     *
     * @param label Vertex label.
     * @return Gremlin query that gets vertex.
     */
    public static String getVertexQuery(final String label) {
        return String.format("g.V().hasLabel(\"%s\").valueMap().by(unfold())", label);
    }

    /**
     * Function to construct Gremlin query that drops vertex.
     *
     * @param label Vertex label.
     * @return Gremlin query that drops vertex.
     */
    public static String dropVertexQuery(final String label) {
        return String.format("g.V().hasLabel(\"%s\").drop().iterate()", label);
    }

    /**
     * Function that creates vertex.
     *
     * @param connection Gremlin database connection.
     * @param label      Vertex label.
     * @param properties Map containing Vertex properties.
     * @throws SQLException if fails to create vertex.
     */
    public static void createVertex(final Connection connection,
                                    final String label,
                                    final Map<String, Object> properties) throws SQLException {
        connection.createStatement()
                .executeQuery(createVertexQuery(label, properties));
    }

    /**
     * Function that gets vertex.
     *
     * @param connection Gremlin database connection.
     * @param label      Vertex label.
     * @return Gremlin result set containing vertex.
     * @throws SQLException if fails to get vertex.
     */
    public static GremlinResultSet getVertex(final Connection connection,
                                             final String label) throws SQLException {
        return (GremlinResultSet) connection.createStatement()
                .executeQuery(getVertexQuery(label));
    }

    /**
     * Function that drops vertex.
     *
     * @param connection Gremlin database connection.
     * @param label      Vertex label.
     * @throws SQLException if fails to drop vertex.
     */
    public static void dropVertex(final Connection connection,
                                  final String label) throws SQLException {
        connection.createStatement().executeQuery(dropVertexQuery(label));
    }

    /**
     * Function that gets all data from database.
     *
     * @param connection Gremlin database connection.
     * @return Gremlin result set containing vertex.
     * @throws SQLException if fails to get data.
     */
    public static GremlinResultSet getAll(final Connection connection) throws SQLException {
        return (GremlinResultSet) connection.createStatement()
                .executeQuery("g.V().valueMap().by(unfold())");
    }

    /**
     * Function that drops all data from database.
     *
     * @param connection Gremlin database connection.
     * @throws SQLException if fails to drop data.
     */
    public static void dropAll(final Connection connection) throws SQLException {
        connection.createStatement().executeQuery("g.V().drop().iterate()");
    }
}
