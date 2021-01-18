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
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JavaToJdbcTypeConverter {
    public static final Map<Class<?>, ClassConverter<?>> CLASS_CONVERTER_MAP = new HashMap<>();
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JavaToJdbcTypeConverter.class);
    private static final Map<String, Boolean> BOOLEAN_STRINGS = ImmutableMap.of(
            "1", true, "true", true,
            "0", false, "false", false);
    private static final Calendar DEFAULT_CALENDAR = new GregorianCalendar();

    static {
        CLASS_CONVERTER_MAP.put(Boolean.class, JavaToJdbcTypeConverter::toBoolean);
        CLASS_CONVERTER_MAP.put(Byte.class, JavaToJdbcTypeConverter::toByte);
        CLASS_CONVERTER_MAP.put(Short.class, JavaToJdbcTypeConverter::toShort);
        CLASS_CONVERTER_MAP.put(Integer.class, JavaToJdbcTypeConverter::toInteger);
        CLASS_CONVERTER_MAP.put(Long.class, JavaToJdbcTypeConverter::toLong);
        CLASS_CONVERTER_MAP.put(Float.class, JavaToJdbcTypeConverter::toFloat);
        CLASS_CONVERTER_MAP.put(Double.class, JavaToJdbcTypeConverter::toDouble);
        CLASS_CONVERTER_MAP.put(byte[].class, JavaToJdbcTypeConverter::toByteArray);
        CLASS_CONVERTER_MAP.put(String.class, JavaToJdbcTypeConverter::toString);
        CLASS_CONVERTER_MAP.put(Date.class, JavaToJdbcTypeConverter::toDate);
        CLASS_CONVERTER_MAP.put(Time.class, JavaToJdbcTypeConverter::toTime);
        CLASS_CONVERTER_MAP.put(Timestamp.class, JavaToJdbcTypeConverter::toTimestamp);
    }

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
                input instanceof Map ||
                input instanceof java.sql.Date ||
                input instanceof java.sql.Time ||
                input instanceof java.sql.Timestamp ||
                input instanceof LocalDate ||
                input instanceof LocalTime ||
                input instanceof LocalDateTime ||
                input instanceof OffsetTime ||
                input instanceof ZonedDateTime) {
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

    /**
     * Function that takes in a Java Object and attempts to convert it to a byte array.
     *
     * @param input Input Object.
     * @return byte array.
     * @throws SQLException if conversion cannot be performed.
     */
    public static byte[] toByteArray(final Object input) throws SQLException {
        if (input == null) {
            return null;
        }
        if (input instanceof String) {
            return ((String) input).getBytes();
        } else if (input instanceof byte[]) {
            return (byte[]) input;
        } else if (input instanceof Byte) {
            return new byte[] {(Byte) input};
        }
        throw createConversionException(input.getClass(), Double.class);
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Date.
     *
     * @param input Input Object.
     * @param calendar Calendar to use.
     * @return Date.
     * @throws SQLException if conversion cannot be performed.
     */
    public static java.sql.Date toDate(final Object input, final Calendar calendar) throws SQLException {
        if (input == null) {
            return null;
        }

        if (input instanceof Long) {
            return new Date((Long) input);
        } else if (input instanceof String) {
            try {
                getCalendarDate(Date.valueOf((String) input), calendar);
            } catch (final IllegalArgumentException ignored) {
            }
        } else if (input instanceof LocalDate) {
            return getCalendarDate(Date.valueOf((LocalDate) input), calendar);
        } else if (input instanceof LocalDateTime) {
            return getCalendarDate(Date.valueOf(((LocalDateTime) input).toLocalDate()), calendar);
        } else if (input instanceof ZonedDateTime) {
            return getCalendarDate(Date.valueOf(((ZonedDateTime) input).toLocalDate()), calendar);
        }
        throw createConversionException(input.getClass(), java.sql.Date.class);
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Time.
     *
     * @param input Input Object.
     * @param calendar Calendar to use.
     * @return Time.
     * @throws SQLException if conversion cannot be performed.
     */
    public static java.sql.Time toTime(final Object input, final Calendar calendar) throws SQLException {
        if (input == null) {
            return null;
        }

        try {
            if (input instanceof Long) {
                return getCalendarTime(new Time((Long) input), calendar);
            } else if (input instanceof String) {
                try {
                    return getCalendarTime(Time.valueOf((String) input), calendar);
                } catch (final IllegalArgumentException ignored) {
                }
            } else if (input instanceof LocalTime) {
                return getCalendarTime(Time.valueOf((LocalTime) input), calendar);
            } else if (input instanceof LocalDateTime) {
                return getCalendarTime(Time.valueOf(((LocalDateTime) input).toLocalTime()), calendar);
            } else if (input instanceof ZonedDateTime) {
                return getCalendarTime(Time.valueOf(((ZonedDateTime) input).toLocalTime()), calendar);
            } else if (input instanceof OffsetTime) {
                return getCalendarTime(Time.valueOf(((OffsetTime) input).toLocalTime()), calendar);
            }
        } catch (final Exception ignored) {
        }
        throw createConversionException(input.getClass(), java.sql.Date.class);
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Timestamp.
     *
     * @param input Input Object.
     * @param calendar Calendar to use.
     * @return Timestamp.
     * @throws SQLException if conversion cannot be performed.
     */
    public static java.sql.Timestamp toTimestamp(final Object input, final Calendar calendar) throws SQLException {
        if (input == null) {
            return null;
        }

        try {
            if (input instanceof Long) {
                return getCalendarTimestamp(new Timestamp((Long) input), calendar);
            } else if (input instanceof String) {
                try {
                    return getCalendarTimestamp(Timestamp.valueOf((String) input), calendar);
                } catch (final IllegalArgumentException ignored) {
                }
            } else if (input instanceof LocalDateTime) {
                return getCalendarTimestamp(Timestamp.valueOf((LocalDateTime) input), calendar);
            } else if (input instanceof LocalDate) {
                return getCalendarTimestamp(Timestamp.valueOf(((LocalDate) input).atStartOfDay()), calendar);
            } else if (input instanceof LocalTime) {
                return getCalendarTimestamp(Timestamp.valueOf(((LocalTime) input).atDate(LocalDate.ofEpochDay(0))),
                        calendar);
            } else if (input instanceof ZonedDateTime) {
                return getCalendarTimestamp(Timestamp.valueOf(((ZonedDateTime) input).toLocalDateTime()), calendar);
            } else if (input instanceof OffsetTime) {
                return getCalendarTimestamp(
                        Timestamp.valueOf(((OffsetTime) input).toLocalTime().atDate(LocalDate.ofEpochDay(0))),
                        calendar);
            }
        } catch (final Exception ignored) {
        }
        throw createConversionException(input.getClass(), java.sql.Date.class);
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Time.
     *
     * @param input Input Object.
     * @return Time.
     * @throws SQLException if conversion cannot be performed.
     */
    public static java.sql.Time toTime(final Object input) throws SQLException {
        return toTime(input, DEFAULT_CALENDAR);
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Date.
     *
     * @param input Input Object.
     * @return Date.
     * @throws SQLException if conversion cannot be performed.
     */
    public static java.sql.Date toDate(final Object input) throws SQLException {
        return toDate(input, DEFAULT_CALENDAR);
    }

    /**
     * Function that takes in a Java Object and attempts to convert it to a Timestamp.
     *
     * @param input Input Object.
     * @return Timestamp.
     * @throws SQLException if conversion cannot be performed.
     */
    public static java.sql.Timestamp toTimestamp(final Object input) throws SQLException {
        return toTimestamp(input, DEFAULT_CALENDAR);
    }

    private static Date getCalendarDate(final Date date, final Calendar calendar) {
        final Instant zdtInstant = date.toLocalDate().atStartOfDay(calendar.getTimeZone().toZoneId()).toInstant();
        return new Date(zdtInstant.toEpochMilli());
    }

    private static Time getCalendarTime(final Time time, final Calendar calendar) {
        final LocalDateTime localDateTime = time.toLocalTime().atDate(LocalDate.of(1970, 1, 1));
        final ZoneId zonedDateTime = ZoneId.from(localDateTime.atZone(calendar.getTimeZone().toZoneId()));
        return new Time(localDateTime.atZone(zonedDateTime).toInstant().toEpochMilli());
    }

    private static Timestamp getCalendarTimestamp(final Timestamp timestamp, final Calendar calendar) {
        final Instant instant = timestamp.toLocalDateTime().atZone(calendar.getTimeZone().toZoneId()).toInstant();
        final Timestamp timestampAdjusted = new Timestamp(instant.toEpochMilli());
        timestampAdjusted.setNanos(instant.getNano());
        return timestampAdjusted;
    }

    private static SQLException createConversionException(final Class source, final Class target) {
        return SqlError.createSQLException(
                LOGGER,
                SqlState.DATA_EXCEPTION,
                SqlError.UNSUPPORTED_CONVERSION,
                source.getTypeName(),
                target.getTypeName());
    }

    /**
     * Lambda function interface to convert Object to template type.
     *
     * @param <T> Template type to convert to.
     */
    public interface ClassConverter<T> {
        /**
         * Lambda function to perform conversion.
         *
         * @param input Input Object.
         * @return Template type to convert to.
         * @throws SQLException If error is encountered during conversion.
         */
        T function(final Object input) throws SQLException;
    }
}
