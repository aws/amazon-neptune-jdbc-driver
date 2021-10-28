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

package software.aws.neptune.jdbc.utilities;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.beanutils.converters.AbstractConverter;
import org.apache.commons.beanutils.converters.ArrayConverter;
import org.apache.commons.beanutils.converters.BigDecimalConverter;
import org.apache.commons.beanutils.converters.BooleanConverter;
import org.apache.commons.beanutils.converters.ByteConverter;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.beanutils.converters.DoubleConverter;
import org.apache.commons.beanutils.converters.FloatConverter;
import org.apache.commons.beanutils.converters.IntegerConverter;
import org.apache.commons.beanutils.converters.LongConverter;
import org.apache.commons.beanutils.converters.ShortConverter;
import org.apache.commons.beanutils.converters.SqlTimestampConverter;
import org.apache.commons.beanutils.converters.StringConverter;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
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
import java.util.Map;

public class JavaToJdbcTypeConverter {
    public static final Map<Class<?>, Integer> CLASS_TO_JDBC_ORDINAL = new HashMap<>();
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JavaToJdbcTypeConverter.class);
    private static final Map<String, Boolean> BOOLEAN_STRINGS = ImmutableMap.of(
            "1", true, "true", true,
            "0", false, "false", false);
    private static final Calendar DEFAULT_CALENDAR = new GregorianCalendar();

    private static final ImmutableMap<Class<?>, AbstractConverter> TYPE_CONVERTERS_MAP;

    static {
        CLASS_TO_JDBC_ORDINAL.put(Boolean.class, JdbcType.BIT.getJdbcCode());
        CLASS_TO_JDBC_ORDINAL.put(Byte.class, JdbcType.TINYINT.getJdbcCode());
        CLASS_TO_JDBC_ORDINAL.put(Short.class, JdbcType.SMALLINT.getJdbcCode());
        CLASS_TO_JDBC_ORDINAL.put(Integer.class, JdbcType.INTEGER.getJdbcCode());
        CLASS_TO_JDBC_ORDINAL.put(Long.class, JdbcType.BIGINT.getJdbcCode());
        CLASS_TO_JDBC_ORDINAL.put(Float.class, JdbcType.REAL.getJdbcCode());
        CLASS_TO_JDBC_ORDINAL.put(Double.class, JdbcType.DOUBLE.getJdbcCode());
        CLASS_TO_JDBC_ORDINAL.put(Date.class, JdbcType.DATE.getJdbcCode());
        CLASS_TO_JDBC_ORDINAL.put(Time.class, JdbcType.TIME.getJdbcCode());
        CLASS_TO_JDBC_ORDINAL.put(String.class, JdbcType.VARCHAR.getJdbcCode());
        CLASS_TO_JDBC_ORDINAL.put(java.math.BigDecimal.class, JdbcType.DECIMAL.getJdbcCode());

        TYPE_CONVERTERS_MAP = ImmutableMap.<Class<?>, AbstractConverter>builder()
                .put(BigDecimal.class, new BigDecimalConverter(0))
                .put(Boolean.class, new BooleanConverter(false))
                .put(boolean.class, new BooleanConverter(false))
                .put(Byte.class, new ByteConverter(0))
                .put(byte.class, new ByteConverter(0))
                .put(Date.class, new DateConverter(null))
                .put(java.util.Date.class, new DateConverter(null))
                .put(Double.class, new DoubleConverter(0.0))
                .put(double.class, new DoubleConverter(0.0))
                .put(Float.class, new FloatConverter(0.0))
                .put(float.class, new FloatConverter(0.0))
                .put(Integer.class, new IntegerConverter(0))
                .put(int.class, new IntegerConverter(0))
                .put(Long.class, new LongConverter(0))
                .put(long.class, new LongConverter(0))
                .put(Short.class, new ShortConverter(0))
                .put(short.class, new ShortConverter(0))
                .put(String.class, new StringConverter())
                .put(Timestamp.class, new SqlTimestampConverter())
                .put(Byte[].class, new ArrayConverter(Byte[].class, new ByteConverter()))
                .put(byte[].class, new ArrayConverter(byte[].class, new ByteConverter()))
                .build();
    }

    /**
     * Gets the type converter for the given source type.
     *
     * @param sourceType the source type to get the converter for.
     * @param targetType the target type used to log error in case of missing converter.
     * @return a {@link AbstractConverter} instance for the source type.
     *
     * @throws SQLException if a converter cannot be found the source type.
     */
    public static AbstractConverter get(final Class<? extends Object> sourceType,
                                        final Class<? extends Object> targetType) throws SQLException {
        final AbstractConverter converter = TYPE_CONVERTERS_MAP.get(sourceType);
        if (converter == null) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.UNSUPPORTED_CONVERSION,
                    sourceType.getSimpleName(),
                    targetType.getSimpleName());
        }
        return converter;
    }
}
