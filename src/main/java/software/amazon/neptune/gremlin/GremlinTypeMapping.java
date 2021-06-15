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

package software.amazon.neptune.gremlin;

import software.amazon.jdbc.utilities.JdbcType;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Gremlin type mapping class to simplify type conversion and mapping.
 */
public class GremlinTypeMapping {
    public static final Map<Class<?>, JdbcType> GREMLIN_TO_JDBC_TYPE_MAP = new HashMap<>();

    static {
        GREMLIN_TO_JDBC_TYPE_MAP.put(String.class, JdbcType.VARCHAR);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Boolean.class, JdbcType.BIT);
        GREMLIN_TO_JDBC_TYPE_MAP.put(byte[].class, JdbcType.VARCHAR);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Byte.class, JdbcType.TINYINT);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Short.class, JdbcType.SMALLINT);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Integer.class, JdbcType.INTEGER);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Long.class, JdbcType.BIGINT);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Float.class, JdbcType.FLOAT);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Double.class, JdbcType.DOUBLE);
        GREMLIN_TO_JDBC_TYPE_MAP.put(java.util.Date.class, JdbcType.DATE);
        GREMLIN_TO_JDBC_TYPE_MAP.put(java.sql.Date.class, JdbcType.DATE);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Time.class, JdbcType.TIME);
        GREMLIN_TO_JDBC_TYPE_MAP.put(Timestamp.class, JdbcType.TIMESTAMP);
    }

    /**
     * Function to get JDBC type equivalent of Gremlin input type.
     *
     * @param gremlinClass Gremlin class type.
     * @return JDBC equivalent for Gremlin class type.
     */
    public static JdbcType getJDBCType(final Class<?> gremlinClass) {
        return GREMLIN_TO_JDBC_TYPE_MAP.getOrDefault(gremlinClass, JdbcType.VARCHAR);
    }

    /**
     * Function to check if Gremlin has a direct converter for the given class type.
     *
     * @param gremlinClass Input class type.
     * @return True if a direct converter exists, false otherwise.
     */
    public static boolean checkContains(final Class<?> gremlinClass) {
        return GREMLIN_TO_JDBC_TYPE_MAP.containsKey(gremlinClass);
    }
}
