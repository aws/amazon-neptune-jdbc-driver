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
 */

package software.amazon.jdbc.utilities;

import com.google.common.collect.ImmutableMap;
import org.slf4j.LoggerFactory;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class JavaToJdbcTypeConverter {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JavaToJdbcTypeConverter.class);
    private static final Map<String, Boolean> BOOLEAN_STRINGS = ImmutableMap.of(
            "1", true, "true", true,
            "0", false, "false", false);

    /**
     * Function that takes in a Java Object and attempts to convert it to a Boolean.
     *
     * @param input Input Object.
     * @return Boolean value.
     * @throws SQLException if conversion cannot be performed.
     */
    public static Boolean toBoolean(final Object input) throws SQLException {
        if (input == null) {
            return false;
        }
        if (input instanceof Boolean) {
            return (Boolean) input;
        } else if (input instanceof Byte ||
                input instanceof Short ||
                input instanceof Integer ||
                input instanceof Long) {
            try {
                return toLong(input) != 0;
            } catch (final SQLException ignored) {
            }
        } else if (input instanceof String) {
            final String strInput = ((String) input).toLowerCase();
            if (BOOLEAN_STRINGS.containsKey(strInput)) {
                return BOOLEAN_STRINGS.get(strInput);
            }
        }
        throw createConversionException(input.getClass(), Boolean.class);
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Byte.
     *
     * @param input Input Object.
     * @return Byte value.
     * @throws SQLException if conversion cannot be performed.
     */
    public static Byte toByte(final Object input) throws SQLException {
        if (input == null) {
            return 0;
        }
        try {
            return toLong(input).byteValue();
        } catch (final SQLException ignored) {
            throw createConversionException(input.getClass(), Byte.class);
        }
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Short.
     *
     * @param input Input Object.
     * @return Short value.
     * @throws SQLException if conversion cannot be performed.
     */
    public static Short toShort(final Object input) throws SQLException {
        if (input == null) {
            return 0;
        }
        try {
            return toLong(input).shortValue();
        } catch (final SQLException ignored) {
            throw createConversionException(input.getClass(), Short.class);
        }
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Integer.
     *
     * @param input Input Object.
     * @return Integer value.
     * @throws SQLException if conversion cannot be performed.
     */
    public static Integer toInteger(final Object input) throws SQLException {
        if (input == null) {
            return 0;
        }
        try {
            return toLong(input).intValue();
        } catch (final SQLException ignored) {
            throw createConversionException(input.getClass(), Integer.class);
        }
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Long.
     *
     * @param input Input Object.
     * @return Long value.
     * @throws SQLException if conversion cannot be performed.
     */
    public static Long toLong(final Object input) throws SQLException {
        if (input == null) {
            return 0L;
        }
        if (input instanceof Double) {
            return ((Double) input).longValue();
        } else if (input instanceof Float) {
            return ((Float) input).longValue();
        } else if (input instanceof Long) {
            return (Long) input;
        } else if (input instanceof Integer) {
            return ((Integer) input).longValue();
        } else if (input instanceof Short) {
            return ((Short) input).longValue();
        } else if (input instanceof Byte) {
            return ((Byte) input).longValue();
        } else if (input instanceof String) {
            try {
                return Long.parseLong((String) input);
            } catch (final NumberFormatException ignored) {
            }
        }
        throw createConversionException(input.getClass(), Long.class);
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a String.
     *
     * @param input Input Object.
     * @return String value.
     * @throws SQLException if conversion cannot be performed.
     */
    public static String toString(final Object input) throws SQLException {
        if (input == null) {
            return null;
        }
        if (input instanceof String) {
            return (String) input;
        } else if (input instanceof Boolean ||
                input instanceof Byte ||
                input instanceof Short ||
                input instanceof Integer ||
                input instanceof Long ||
                input instanceof Float ||
                input instanceof Double ||
                input instanceof List ||
                input instanceof Map) {
            return input.toString();
        }
        throw createConversionException(input.getClass(), String.class);
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Float.
     *
     * @param input Input Object.
     * @return Float value.
     * @throws SQLException if conversion cannot be performed.
     */
    public static Float toFloat(final Object input) throws SQLException {
        if (input == null) {
            return 0.0f;
        }
        try {
            return toDouble(input).floatValue();
        } catch (final SQLException ignored) {
            throw createConversionException(input.getClass(), Float.class);
        }
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Double.
     *
     * @param input Input Object.
     * @return Double value.
     * @throws SQLException if conversion cannot be performed.
     */
    public static Double toDouble(final Object input) throws SQLException {
        if (input == null) {
            return 0.0;
        }
        if (input instanceof Double) {
            return (Double) input;
        } else if (input instanceof Float) {
            return ((Float) input).doubleValue();
        } else if (input instanceof Long) {
            return ((Long) input).doubleValue();
        } else if (input instanceof Integer) {
            return ((Integer) input).doubleValue();
        } else if (input instanceof Short) {
            return ((Short) input).doubleValue();
        } else if (input instanceof Byte) {
            return ((Byte) input).doubleValue();
        } else if (input instanceof String) {
            try {
                return Double.parseDouble((String) input);
            } catch (final NumberFormatException ignored) {
            }
        }
        throw createConversionException(input.getClass(), Double.class);
    }

    private static SQLException createConversionException(final Class source, final Class target) {
        return SqlError.createSQLException(
                LOGGER,
                SqlState.DATA_EXCEPTION,
                SqlError.UNSUPPORTED_CONVERSION,
                source.getTypeName(),
                target.getTypeName());
    }
}
