/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
 
package software.aws.neptune.gremlin.resultset;

import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetTypeInfo;

import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GremlinResultSetGetTypeInfo extends ResultSetGetTypeInfo {
    private static final Map<String, Object> STRING_INFO = new HashMap<>();
    private static final Map<String, Object> BOOLEAN_INFO = new HashMap<>();
    private static final Map<String, Object> BYTES_INFO = new HashMap<>();;
    private static final Map<String, Object> BYTE_INFO = new HashMap<>();
    private static final Map<String, Object> SHORT_INFO = new HashMap<>();
    private static final Map<String, Object> INTEGER_INFO = new HashMap<>();
    private static final Map<String, Object> LONG_INFO = new HashMap<>();
    private static final Map<String, Object> FLOAT_INFO = new HashMap<>();
    private static final Map<String, Object> DOUBLE_INFO = new HashMap<>();
    private static final Map<String, Object> UTIL_DATE_INFO = new HashMap<>();
    private static final Map<String, Object> SQL_DATE_INFO = new HashMap<>();
    private static final Map<String, Object> TIME_INFO = new HashMap<>();
    private static final Map<String, Object> TIMESTAMP_INFO = new HashMap<>();

    private static final List<Map<String, Object>> TYPE_INFO = new ArrayList<>();

    static {
        // The order added to TYPE_INFO matters
        BOOLEAN_INFO.put("TYPE_NAME", "Boolean");
        BOOLEAN_INFO.put("DATA_TYPE", Types.BIT);
        BOOLEAN_INFO.put("PRECISION", 1);
        BOOLEAN_INFO.put("LITERAL_PREFIX", null);
        BOOLEAN_INFO.put("LITERAL_SUFFIX", null);
        BOOLEAN_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(BOOLEAN_INFO);

        BYTE_INFO.put("TYPE_NAME", "Byte");
        BYTE_INFO.put("DATA_TYPE", Types.TINYINT);
        BYTE_INFO.put("PRECISION", 3);
        BYTE_INFO.put("LITERAL_PREFIX", null);
        BYTE_INFO.put("LITERAL_SUFFIX", null);
        BYTE_INFO.put("CASE_SENSITIVE", false);
        BYTE_INFO.put("MINIMUM_SCALE", 0);
        BYTE_INFO.put("MAXIMUM_SCALE", 0);
        BYTE_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(BYTE_INFO);

        LONG_INFO.put("TYPE_NAME", "Long");
        LONG_INFO.put("DATA_TYPE", Types.BIGINT);
        LONG_INFO.put("PRECISION", 20);
        LONG_INFO.put("LITERAL_PREFIX", null);
        LONG_INFO.put("LITERAL_SUFFIX", null);
        LONG_INFO.put("CASE_SENSITIVE", false);
        LONG_INFO.put("MINIMUM_SCALE", 0);
        LONG_INFO.put("MAXIMUM_SCALE", 0);
        LONG_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(LONG_INFO);

        INTEGER_INFO.put("TYPE_NAME", "Integer");
        INTEGER_INFO.put("DATA_TYPE", Types.INTEGER);
        INTEGER_INFO.put("PRECISION", 11);
        INTEGER_INFO.put("LITERAL_PREFIX", null);
        INTEGER_INFO.put("LITERAL_SUFFIX", null);
        INTEGER_INFO.put("CASE_SENSITIVE", false);
        INTEGER_INFO.put("MINIMUM_SCALE", 0);
        INTEGER_INFO.put("MAXIMUM_SCALE", 0);
        INTEGER_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(INTEGER_INFO);

        SHORT_INFO.put("TYPE_NAME", "Short");
        SHORT_INFO.put("DATA_TYPE", Types.SMALLINT);
        SHORT_INFO.put("PRECISION", 5);
        SHORT_INFO.put("LITERAL_PREFIX", null);
        SHORT_INFO.put("LITERAL_SUFFIX", null);
        SHORT_INFO.put("CASE_SENSITIVE", false);
        INTEGER_INFO.put("MINIMUM_SCALE", 0);
        INTEGER_INFO.put("MAXIMUM_SCALE", 0);
        INTEGER_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(SHORT_INFO);

        FLOAT_INFO.put("TYPE_NAME", "Float");
        FLOAT_INFO.put("DATA_TYPE", Types.REAL);
        FLOAT_INFO.put("PRECISION", 15);
        FLOAT_INFO.put("LITERAL_PREFIX", null);
        FLOAT_INFO.put("LITERAL_SUFFIX", null);
        FLOAT_INFO.put("CASE_SENSITIVE", false);
        FLOAT_INFO.put("MINIMUM_SCALE", 0);
        FLOAT_INFO.put("MAXIMUM_SCALE", 0);
        FLOAT_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(FLOAT_INFO);

        DOUBLE_INFO.put("TYPE_NAME", "Double");
        DOUBLE_INFO.put("DATA_TYPE", Types.DOUBLE);
        DOUBLE_INFO.put("PRECISION", 15);
        DOUBLE_INFO.put("LITERAL_PREFIX", null);
        DOUBLE_INFO.put("LITERAL_SUFFIX", null);
        DOUBLE_INFO.put("CASE_SENSITIVE", false);
        DOUBLE_INFO.put("MINIMUM_SCALE", 0);
        DOUBLE_INFO.put("MAXIMUM_SCALE", 0);
        DOUBLE_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(DOUBLE_INFO);

        STRING_INFO.put("TYPE_NAME", "String");
        STRING_INFO.put("DATA_TYPE", Types.VARCHAR);
        STRING_INFO.put("PRECISION", Integer.MAX_VALUE);
        STRING_INFO.put("LITERAL_PREFIX", "'");
        STRING_INFO.put("LITERAL_SUFFIX", "'");
        STRING_INFO.put("CASE_SENSITIVE", true);
        TYPE_INFO.add(STRING_INFO);

        BYTES_INFO.put("TYPE_NAME", "byte[]");
        BYTES_INFO.put("DATA_TYPE", Types.VARCHAR);
        BYTES_INFO.put("PRECISION", Integer.MAX_VALUE);
        BYTES_INFO.put("LITERAL_PREFIX", null);
        BYTES_INFO.put("LITERAL_SUFFIX", null);
        BYTES_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(BYTES_INFO);

        SQL_DATE_INFO.put("TYPE_NAME", "sql.Date");
        SQL_DATE_INFO.put("DATA_TYPE", Types.DATE);
        SQL_DATE_INFO.put("PRECISION", 24);
        SQL_DATE_INFO.put("LITERAL_PREFIX", null);
        SQL_DATE_INFO.put("LITERAL_SUFFIX", null);
        SQL_DATE_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(SQL_DATE_INFO);

        UTIL_DATE_INFO.put("TYPE_NAME", "util.Date");
        UTIL_DATE_INFO.put("DATA_TYPE", Types.DATE);
        UTIL_DATE_INFO.put("PRECISION", 24);
        UTIL_DATE_INFO.put("LITERAL_PREFIX", null);
        UTIL_DATE_INFO.put("LITERAL_SUFFIX", null);
        UTIL_DATE_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(UTIL_DATE_INFO);

        TIME_INFO.put("TYPE_NAME", "Time");
        TIME_INFO.put("DATA_TYPE", Types.TIME);
        TIME_INFO.put("PRECISION", 24);
        TIME_INFO.put("LITERAL_PREFIX", null);
        TIME_INFO.put("LITERAL_SUFFIX", null);
        TIME_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(TIME_INFO);

        TIMESTAMP_INFO.put("TYPE_NAME", "Timestamp");
        TIMESTAMP_INFO.put("DATA_TYPE", Types.TIMESTAMP);
        TIMESTAMP_INFO.put("PRECISION", 24);
        TIMESTAMP_INFO.put("LITERAL_PREFIX", null);
        TIMESTAMP_INFO.put("LITERAL_SUFFIX", null);
        TIMESTAMP_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(TIMESTAMP_INFO);

        populateConstants(TYPE_INFO);
    }

    public GremlinResultSetGetTypeInfo(final Statement statement) {
        super(statement, new ArrayList<>(TYPE_INFO));
    }
}
