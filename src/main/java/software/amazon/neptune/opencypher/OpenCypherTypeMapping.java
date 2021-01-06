/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.neptune.opencypher;

import org.neo4j.driver.Value;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * OpenCypher type mapping class to simplify type conversion and mapping.
 */
public class OpenCypherTypeMapping {
    public static final Map<Type, Integer> BOLT_TO_JDBC_TYPE_MAP = new HashMap<>();
    public static final Map<Type, Class> BOLT_TO_JAVA_TYPE_MAP = new HashMap<>();
    public static final Map<Type, Converter> BOLT_TO_JAVA_TRANSFORM_MAP = new HashMap<>();

    static {
        // Bolt->JDBC mapping.
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.ANY(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BOOLEAN(), Types.BIT);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BYTES(), Types.VARCHAR); // TODO: Revisit
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.STRING(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP
                .put(InternalTypeSystem.TYPE_SYSTEM.NUMBER(), Types.DOUBLE); // TODO double check this is floating point
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.INTEGER(), Types.BIGINT);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.FLOAT(), Types.DOUBLE);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LIST(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.MAP(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NODE(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.RELATIONSHIP(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.PATH(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.POINT(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP
                .put(InternalTypeSystem.TYPE_SYSTEM.DATE(), Types.DATE); // TODO: Look into datetime more closely.
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.TIME(),
                Types.TIME); // TODO: Scope only says dates, do we need time/etc?
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_TIME(), Types.TIME);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME(), Types.TIMESTAMP);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE_TIME(), Types.TIMESTAMP);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DURATION(), Types.VARCHAR);
        BOLT_TO_JDBC_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NULL(), Types.NULL);

        // Bolt->Java mapping.
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.ANY(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BOOLEAN(), Boolean.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BYTES(), String.class); // TODO: Revisit
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.STRING(), String.class);
        BOLT_TO_JAVA_TYPE_MAP
                .put(InternalTypeSystem.TYPE_SYSTEM.NUMBER(), Double.class); // TODO double check this is floating point
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.INTEGER(), Long.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.FLOAT(), Double.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LIST(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.MAP(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NODE(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.RELATIONSHIP(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.PATH(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.POINT(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE(),
                java.sql.Date.class); // TODO: Look into datetime more closely.
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.TIME(),
                java.sql.Time.class); // TODO: Scope only says dates, do we need time/etc?
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_TIME(), java.sql.Time.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME(), java.sql.Timestamp.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE_TIME(), java.sql.Timestamp.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DURATION(), String.class);
        BOLT_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NULL(), Object.class);

        BOLT_TO_JAVA_TRANSFORM_MAP.put(null, (Value v) -> null);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.ANY(), Value::toString);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BOOLEAN(), Value::asBoolean);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.BYTES(), Value::asString); // TODO: Revisit
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.STRING(), Value::asString);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NUMBER(),
                Value::asDouble); // TODO double check this is floating point
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.INTEGER(), Value::asLong);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.FLOAT(), Value::asDouble);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LIST(), Value::asList);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.MAP(), Value::asMap);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.NODE(), (Value v) -> null);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.RELATIONSHIP(), Value::asString);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.PATH(), (Value v) -> null);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.POINT(), (Value v) -> null);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE(),
                Value::asLocalDate); // TODO: Look into datetime more closely.
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.TIME(),
                Value::asLocalTime); // TODO: Scope only says dates, do we need time/etc?
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_TIME(), Value::asLocalTime);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME(), Value::asLocalDateTime);
        BOLT_TO_JAVA_TRANSFORM_MAP.put(InternalTypeSystem.TYPE_SYSTEM.DATE_TIME(), Value::asLocalDateTime);
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
}
