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

package software.aws.neptune.opencypher;

import org.neo4j.driver.Value;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.types.Type;
import software.aws.neptune.jdbc.utilities.JdbcType;
import java.util.HashMap;
import java.util.Map;

/**
 * OpenCypher type mapping class to simplify type conversion and mapping.
 */
public class OpenCypherTypeMapping {
    public static final Map<Type, JdbcType> BOLT_TO_JDBC_TYPE_MAP = new HashMap<>();
    public static final Map<Type, Class<?>> BOLT_TO_JAVA_TYPE_MAP = new HashMap<>();
    public static final Map<Type, Converter<?>> BOLT_TO_JAVA_TRANSFORM_MAP = new HashMap<>();
    public static final Converter<String> NODE_CONVERTER = new NodeConverter();
    public static final Converter<String> RELATIONSHIP_CONVERTER = new RelationshipConverter();
    public static final Converter<String> PATH_CONVERTER = new PathConverter();
    public static final Converter<String> POINT_CONVERTER = new PointConverter();

    static {
        // Bolt->JDBC mapping.
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.ANY(), JdbcType.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BOOLEAN(), JdbcType.BIT);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BYTES(), JdbcType.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.STRING(), JdbcType.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NUMBER(), JdbcType.DOUBLE);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.INTEGER(), JdbcType.BIGINT);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.FLOAT(), JdbcType.DOUBLE);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LIST(), JdbcType.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.MAP(), JdbcType.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NODE(), JdbcType.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.RELATIONSHIP(), JdbcType.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.PATH(), JdbcType.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.POINT(), JdbcType.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE(), JdbcType.DATE);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.TIME(), JdbcType.TIME);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_TIME(), JdbcType.TIME);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME(), JdbcType.TIMESTAMP);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE_TIME(), JdbcType.TIMESTAMP);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DURATION(), JdbcType.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NULL(), JdbcType.NULL);

        // Bolt->Java mapping.
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.ANY(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BOOLEAN(), Boolean.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BYTES(), byte[].class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.STRING(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NUMBER(), Double.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.INTEGER(), Long.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.FLOAT(), Double.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LIST(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.MAP(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NODE(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.RELATIONSHIP(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.PATH(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.POINT(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE(), java.sql.Date.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.TIME(), java.sql.Time.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_TIME(), java.sql.Time.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME(), java.sql.Timestamp.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE_TIME(), java.sql.Timestamp.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DURATION(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NULL(), Object.class);

        BOLT_TO_JAVA_TRANSFORM_MAP.put(null, (Value v) -> null);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.ANY(), Value::toString);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BOOLEAN(), Value::asBoolean);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BYTES(), Value::asByteArray);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.STRING(), Value::asString);
        BOLT_TO_JAVA_TRANSFORM_MAP
                .put(InternalTypeSystem.TYPE_SYSTEM.NUMBER(), (Value v) -> v.asNumber().doubleValue());
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.INTEGER(), Value::asLong);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.FLOAT(), Value::asDouble);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LIST(), Value::asList);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.MAP(), Value::asMap);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NODE(), NODE_CONVERTER);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.RELATIONSHIP(), RELATIONSHIP_CONVERTER);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.PATH(), PATH_CONVERTER);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.POINT(), POINT_CONVERTER);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE(), Value::asLocalDate);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.TIME(), Value::asOffsetTime);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_TIME(), Value::asLocalTime);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME(), Value::asLocalDateTime);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE_TIME(), Value::asZonedDateTime);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DURATION(), Value::asIsoDuration);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NULL(), (Value v) -> null);
    }

    /**
     * Converter interface to convert a Value type to a Java type.
     *
     * @param <T> Java type to convert to.
     */
    public interface Converter<T> {
        /**
         * Function to perform conversion.
         *
         * @param value Input value to convert.
         * @return Converted value.
         */
        T convert(Value value);
    }

    static class PointConverter implements Converter<String> {
        @Override
        public String convert(final Value value) {
            return convert(value.asPoint());
        }

        public String convert(final Point value) {
            return Double.isNaN(value.z()) ?
                    String.format("(%f, %f)", value.x(), value.y()) :
                    String.format("(%f, %f, %f)", value.x(), value.y(), value.z());
        }
    }

    static class NodeConverter implements Converter<String> {
        @Override
        public String convert(final Value value) {
            return convert(value.asNode());
        }

        public String convert(final Node value) {
            // Nodes in OpenCypher typically are wrapped in parenthesis, ex. (node)-[relationship]->(node)
            return String.format("(%s : %s)", value.labels().toString(), value.asMap().toString());
        }
    }

    static class RelationshipConverter implements Converter<String> {
        @Override
        public String convert(final Value value) {
            return convert(value.asRelationship());
        }

        public String convert(final Relationship value) {
            // Relationships in OpenCypher typically are wrapped in brackets, ex. (node)-[relationship]->(node)
            return String.format("[%s : %s]", value.type(), value.asMap());
        }
    }

    static class PathConverter implements Converter<String> {
        @Override
        public String convert(final Value value) {
            final StringBuilder stringBuilder = new StringBuilder();
            value.asPath().iterator().forEachRemaining(segment -> {
                final Relationship relationship = segment.relationship();
                final Node start = segment.start();
                final Node end = segment.end();

                // If rel start == start node, direction is (start)-[rel]->(end)
                // Else direction is (start)<-[rel]-(end)
                final String format = (relationship.startNodeId() == start.id()) ? "-%s->%s" : "<-%s-%s";

                // Append start if this is the first node.
                if (stringBuilder.length() == 0) {
                    stringBuilder.append(((NodeConverter) NODE_CONVERTER).convert(start));
                }

                // Add relationship and end node of segment.
                stringBuilder.append(String.format(format,
                        ((RelationshipConverter) RELATIONSHIP_CONVERTER).convert(relationship),
                        ((NodeConverter) NODE_CONVERTER).convert(end)));
            });
            return stringBuilder.toString();
        }
    }
}
