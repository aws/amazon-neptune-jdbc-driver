/*
 * Copyright <2022> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.sparql;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.rdf.model.Literal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.jdbc.utilities.JdbcType;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

public class SparqlTypeMapping {
    public static final Map<Class<?>, JdbcType> SPARQL_JAVA_TO_JDBC_TYPE_MAP = new HashMap<>();
    public static final Map<XSDDatatype, Class<?>> SPARQL_LITERAL_TO_JAVA_TYPE_MAP = new HashMap<>();
    public static final Map<XSDDatatype, JdbcType> SPARQL_LITERAL_TO_JDBC_TYPE_MAP = new HashMap<>();
    public static final Map<XSDDatatype, SparqlTypeMapping.Converter<?>> SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP =
            new HashMap<>();
    public static final Converter<Timestamp> DATE_TIME_CONVERTER = new DateTimeConverter();
    public static final Converter<ZonedDateTime> DATE_TIME_STAMP_CONVERTER = new DateTimeStampConverter();
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlTypeMapping.class);

    static {
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDdecimal, java.math.BigDecimal.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDinteger, java.math.BigInteger.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDnonPositiveInteger, java.math.BigInteger.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDnonNegativeInteger, java.math.BigInteger.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDpositiveInteger, java.math.BigInteger.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDnegativeInteger, java.math.BigInteger.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDbyte, Byte.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDunsignedByte, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDdouble, Double.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDfloat, Float.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDlong, Long.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDunsignedShort, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDunsignedInt, java.math.BigInteger.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDunsignedLong, java.math.BigInteger.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDint, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDshort, Short.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDboolean, Boolean.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDdate, java.sql.Date.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDtime, java.sql.Time.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDdateTime, java.sql.Timestamp.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDdateTimeStamp, java.sql.Timestamp.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDduration, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDstring, String.class);

        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP
                .put(XSDDatatype.XSDtime, (Object value) -> Time.valueOf(getStringValueBasedOnType(value)));
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP
                .put(XSDDatatype.XSDdate, (Object value) -> Date.valueOf(getStringValueBasedOnType(value)));
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDdateTime, DATE_TIME_CONVERTER);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDdateTimeStamp, DATE_TIME_STAMP_CONVERTER);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDduration, SparqlTypeMapping::getStringValueBasedOnType);
        // NOTE: Gregorian date types are not supported currently due to incompatibility with java and JDBC datatype,
        // currently returning as String

        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDdecimal, JdbcType.DECIMAL);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDinteger, JdbcType.BIGINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDnonPositiveInteger, JdbcType.BIGINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDnonNegativeInteger, JdbcType.BIGINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDpositiveInteger, JdbcType.BIGINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDnegativeInteger, JdbcType.BIGINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDbyte, JdbcType.TINYINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDunsignedByte, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDdouble, JdbcType.DOUBLE);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDfloat, JdbcType.REAL);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDlong, JdbcType.BIGINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDunsignedShort, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDunsignedInt, JdbcType.BIGINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDunsignedLong, JdbcType.BIGINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDint, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDshort, JdbcType.SMALLINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDboolean, JdbcType.BIT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDdate, JdbcType.DATE);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDtime, JdbcType.TIME);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDdateTime, JdbcType.TIMESTAMP);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDdateTimeStamp, JdbcType.TIMESTAMP);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDduration, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDstring, JdbcType.VARCHAR);

        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(String.class, JdbcType.VARCHAR);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Boolean.class, JdbcType.BIT);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(byte[].class, JdbcType.VARCHAR);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Byte.class, JdbcType.TINYINT);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Short.class, JdbcType.SMALLINT);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Integer.class, JdbcType.INTEGER);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Long.class, JdbcType.BIGINT);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Float.class, JdbcType.REAL);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Double.class, JdbcType.DOUBLE);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(java.util.Date.class, JdbcType.DATE);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(java.sql.Date.class, JdbcType.DATE);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Time.class, JdbcType.TIME);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Timestamp.class, JdbcType.TIMESTAMP);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(ZonedDateTime.class, JdbcType.TIMESTAMP);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(java.math.BigInteger.class, JdbcType.BIGINT);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(java.math.BigDecimal.class, JdbcType.DECIMAL);
    }

    /**
     * Function to get JDBC type equivalent of Sparql input type.
     *
     * @param sparqlClass Sparql Datatype.
     * @return JDBC equivalent for Sparql class type.
     */
    public static JdbcType getJDBCType(final Object sparqlClass) {
        try {
            return SPARQL_LITERAL_TO_JDBC_TYPE_MAP.getOrDefault(sparqlClass, JdbcType.VARCHAR);
        } catch (final ClassCastException e) {
            LOGGER.warn("Value is not of typed literal XSDDatatype, returning as VARCHAR type");
            return JdbcType.VARCHAR;
        }
    }

    /**
     * Function to get Java type equivalent of Sparql input type.
     *
     * @param sparqlClass Sparql Datatype.
     * @return Java equivalent for Sparql class type.
     */
    public static Class<?> getJavaType(final Object sparqlClass) {
        try {
            return SPARQL_LITERAL_TO_JAVA_TYPE_MAP.getOrDefault(sparqlClass, String.class);
        } catch (final ClassCastException e) {
            LOGGER.warn("Value is not of typed literal XSDDatatype, returning as String type");
            return String.class;
        }
    }

    /**
     * Function to check if Sparql has a direct converter for the given class type.
     *
     * @param sparqlDatatype Input class type.
     * @return True if a direct converter exists, false otherwise.
     */
    public static boolean checkContains(final XSDDatatype sparqlDatatype) {
        return SPARQL_LITERAL_TO_JAVA_TYPE_MAP.containsKey(sparqlDatatype);
    }

    /**
     * Function to check if we have a converter for the given sparql class type.
     *
     * @param sparqlDatatype Input class type.
     * @return True if a direct converter exists, false otherwise.
     */
    public static boolean checkConverter(final XSDDatatype sparqlDatatype) {
        return SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.containsKey(sparqlDatatype);
    }

    private static XSDDateTime getDateTimeValueBasedOnType(final Object value) throws SQLException {
        if (value instanceof Literal) {
            final Literal valueLiteral = (Literal) value;
            return (XSDDateTime) valueLiteral.getValue();
        } else if (value instanceof LiteralLabel) {
            final LiteralLabel valueLiteralLabel = (LiteralLabel) value;
            return (XSDDateTime) valueLiteralLabel.getValue();
        } else {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.UNSUPPORTED_TYPE);
        }
    }

    private static String getStringValueBasedOnType(final Object value) throws SQLException {
        if (value instanceof Literal) {
            final Literal valueLiteral = (Literal) value;
            return valueLiteral.getLexicalForm();
        } else if (value instanceof LiteralLabel) {
            final LiteralLabel valueLiteralLabel = (LiteralLabel) value;
            return valueLiteralLabel.getLexicalForm();
        } else {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.UNSUPPORTED_TYPE);
        }
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
        T convert(Object value) throws SQLException;
    }

    static class DateTimeConverter implements Converter<Timestamp> {
        @Override
        public Timestamp convert(final Object value) throws SQLException {
            return convert(getDateTimeValueBasedOnType(value));
        }

        public Timestamp convert(final XSDDateTime value) {
            return new Timestamp(value.asCalendar().getTimeInMillis());
        }
    }

    // Converts from XSD DateTimeStamp to Java ZonedDateTime in UTC
    static class DateTimeStampConverter implements Converter<ZonedDateTime> {
        @Override
        public ZonedDateTime convert(final Object value) throws SQLException {
            return convert(getDateTimeValueBasedOnType(value));
        }

        public ZonedDateTime convert(final XSDDateTime value) {
            final GregorianCalendar calendarTime = (GregorianCalendar) value.asCalendar();
            return calendarTime.toZonedDateTime().withZoneSameInstant(ZoneId.of("UTC"));
        }
    }
}
