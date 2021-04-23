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

package software.amazon.neptune.gremlin;

import software.amazon.jdbc.utilities.JdbcType;
import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Gremlin type mapping class to simplify type conversion and mapping.
 */
public class GremlinTypeMapping {
    /**
     * TODO: This file is likely not required to the extent it was in OpenCypher, will need to implement what is required and strip the rest.
     */

    public static final Map<Class<?>, JdbcType> GREMLIN_TO_JDBC_TYPE_MAP = new HashMap<>();
    public static final Map<Class<?>, Class<?>> GREMLIN_TO_JAVA_TYPE_MAP = new HashMap<>();
    public static final Map<String, Class<?>> GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP = new HashMap<>();

    static {
        // Gremlin->JDBC mapping.
        GREMLIN_TO_JDBC_TYPE_MAP.put(String.class, JdbcType.VARCHAR);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Boolean.class, JdbcType.BIT);
        GREMLIN_TO_JDBC_TYPE_MAP.put(byte[].class, JdbcType.VARCHAR);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Byte.class, JdbcType.TINYINT);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Short.class, JdbcType.SMALLINT);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Integer.class, JdbcType.INTEGER);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Long.class, JdbcType.BIGINT);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Float.class, JdbcType.FLOAT);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Double.class, JdbcType.DOUBLE);
        GREMLIN_TO_JDBC_TYPE_MAP.put(List.class, JdbcType.VARCHAR);
        GREMLIN_TO_JDBC_TYPE_MAP.put(ArrayList.class, JdbcType.VARCHAR);
        GREMLIN_TO_JDBC_TYPE_MAP.put(LinkedList.class, JdbcType.VARCHAR);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Map.class, JdbcType.VARCHAR);
        GREMLIN_TO_JDBC_TYPE_MAP.put(HashMap.class, JdbcType.VARCHAR);
        GREMLIN_TO_JDBC_TYPE_MAP.put(LinkedHashMap.class, JdbcType.VARCHAR);
        // TODO: Find other types and convert.

        // Gremlin->Java mapping.
        // GREMLIN_TO_JAVA_TYPE_MAP.put(InternalTypeSystem.TYPE_SYSTEM.ANY(), String.class); ??
        GREMLIN_TO_JAVA_TYPE_MAP.put(Boolean.class, Boolean.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(byte[].class, byte[].class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(String.class, String.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(Byte.class, Byte.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(Short.class, Short.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(Integer.class, Integer.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(Long.class, Long.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(Float.class, Float.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(Double.class, Double.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(List.class, String.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(ArrayList.class, String.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(LinkedList.class, String.class);
        // We might need to put special stuff in here to deal with the maps....
        GREMLIN_TO_JAVA_TYPE_MAP.put(Map.class, String.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(HashMap.class, String.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(LinkedHashMap.class, String.class);
        GREMLIN_TO_JAVA_TYPE_MAP.put(null, Object.class);
        // TODO: Find other types and convert.

        // TODO: Find required conversions and implement them

        // TODO: Is this required?
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Byte", Byte.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Short", Short.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Integer", Integer.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Long", Long.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Float", Float.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Double", Double.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("String", String.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Date", Date.class);
        GREMLIN_STRING_TYPE_TO_JAVA_TYPE_CONVERTER_MAP.put("Time", Time.class);
    }
}
