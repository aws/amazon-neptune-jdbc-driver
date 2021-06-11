/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.neptune.sparql;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Literal;
import software.amazon.jdbc.utilities.JdbcType;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

public class SparqlTypeMapping {
    public static final Map<Class<?>, JdbcType> SPARQL_JAVA_TO_JDBC_TYPE_MAP = new HashMap<>();
    public static final Map<Class<?>, Class<?>> SPARQL_TO_JAVA_TYPE_MAP = new HashMap<>();
    public static final Map<Class<?>, SparqlTypeMapping.Converter<?>> SPARQL_TO_JAVA_TRANSFORM_MAP = new HashMap<>();
    public static final Converter<Timestamp> DATE_TIME_CONVERTER = new DateTimeConverter();
    public static final Converter<ZonedDateTime> DATE_TIME_STAMP_CONVERTER = new DateTimeStampConverter();
    public static final Converter<Date> DATE_CONVERTER = new DateConverter();
    public static final Converter<Time> TIME_CONVERTER = new TimeConverter();

    static {
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(String.class, JdbcType.VARCHAR);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Boolean.class, JdbcType.BIT);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(byte[].class, JdbcType.VARCHAR);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Byte.class, JdbcType.TINYINT);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Short.class, JdbcType.SMALLINT);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Integer.class, JdbcType.INTEGER);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Long.class, JdbcType.BIGINT);
        // Should this be JdbcType.REAL?
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Float.class, JdbcType.FLOAT);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Double.class, JdbcType.DOUBLE);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(java.util.Date.class, JdbcType.DATE);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(java.sql.Date.class, JdbcType.DATE);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Time.class, JdbcType.TIME);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(Timestamp.class, JdbcType.TIMESTAMP);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(ZonedDateTime.class, JdbcType.TIMESTAMP);
        // java.math classes to Jdbc directly? BigInteger to BIGINT?
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(java.math.BigInteger.class, JdbcType.BIGINT);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(java.math.BigDecimal.class, JdbcType.DECIMAL);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(org.apache.jena.datatypes.xsd.impl.XSDTimeType.class, JdbcType.TIME);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(org.apache.jena.datatypes.xsd.impl.XSDDateType.class, JdbcType.DATE);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP.put(org.apache.jena.datatypes.xsd.impl.XSDDateTimeType.class, JdbcType.TIMESTAMP);
        SPARQL_JAVA_TO_JDBC_TYPE_MAP
                .put(org.apache.jena.datatypes.xsd.impl.XSDDateTimeStampType.class, JdbcType.TIMESTAMP);

        SPARQL_TO_JAVA_TYPE_MAP.put(org.apache.jena.datatypes.xsd.impl.XSDBaseStringType.class, String.class);
        SPARQL_TO_JAVA_TYPE_MAP.put(org.apache.jena.datatypes.xsd.impl.XSDBaseNumericType.class, Integer.class);
        SPARQL_TO_JAVA_TYPE_MAP.put(org.apache.jena.datatypes.xsd.impl.XSDTimeType.class, java.sql.Time.class);
        SPARQL_TO_JAVA_TYPE_MAP.put(org.apache.jena.datatypes.xsd.impl.XSDDateType.class, java.sql.Date.class);
        SPARQL_TO_JAVA_TYPE_MAP.put(org.apache.jena.datatypes.xsd.impl.XSDDateTimeType.class, java.sql.Timestamp.class);
        SPARQL_TO_JAVA_TYPE_MAP
                .put(org.apache.jena.datatypes.xsd.impl.XSDDateTimeStampType.class, java.sql.Timestamp.class);

        SPARQL_TO_JAVA_TRANSFORM_MAP.put(org.apache.jena.datatypes.xsd.impl.XSDTimeType.class, TIME_CONVERTER);
        SPARQL_TO_JAVA_TRANSFORM_MAP.put(org.apache.jena.datatypes.xsd.impl.XSDDateType.class, DATE_CONVERTER);
        SPARQL_TO_JAVA_TRANSFORM_MAP.put(org.apache.jena.datatypes.xsd.impl.XSDDateTimeType.class, DATE_TIME_CONVERTER);
        SPARQL_TO_JAVA_TRANSFORM_MAP
                .put(org.apache.jena.datatypes.xsd.impl.XSDDateTimeStampType.class, DATE_TIME_STAMP_CONVERTER);

        // NOTE: g* date types are not supported currently due to incompatibility with java and JDBC datatype, currently returning as String
    }

    /**
     * Function to get JDBC type equivalent of Gremlin input type.
     *
     * @param sparqlClass Gremlin class type.
     * @return JDBC equivalent for Gremlin class type.
     */
    public static JdbcType getJDBCType(final Class<?> sparqlClass) {
        return SPARQL_JAVA_TO_JDBC_TYPE_MAP.getOrDefault(sparqlClass, JdbcType.VARCHAR);
    }

    /**
     * Function to check if Sparql has a direct converter for the given class type.
     *
     * @param javaClass Input class type.
     * @return True if a direct converter exists, false otherwise.
     */
    public static boolean checkContains(final Class<?> javaClass) {
        return SPARQL_JAVA_TO_JDBC_TYPE_MAP.containsKey(javaClass);
    }

    /**
     * Function to check if we have a converter for the given sparql class type.
     *
     * @param sparqlClass Input class type.
     * @return True if a direct converter exists, false otherwise.
     */
    public static boolean checkConverter(final Class<?> sparqlClass) {
        return SPARQL_TO_JAVA_TRANSFORM_MAP.containsKey(sparqlClass);
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
        T convert(Literal value);
    }

    static class DateTimeConverter implements Converter<Timestamp> {
        @Override
        public Timestamp convert(final Literal value) {
            return convert((XSDDateTime) value.getValue());
        }

        public Timestamp convert(final XSDDateTime value) {
            return new Timestamp(value.asCalendar().getTimeInMillis());
        }
    }

    // Converts from XSD DateTimeStamp to Java ZonedDateTime in UTC
    static class DateTimeStampConverter implements Converter<ZonedDateTime> {
        @Override
        public ZonedDateTime convert(final Literal value) {
            return convert((XSDDateTime) value.getValue());
        }

        public ZonedDateTime convert(final XSDDateTime value) {
            final GregorianCalendar calendarTime = (GregorianCalendar) value.asCalendar();
            return calendarTime.toZonedDateTime().withZoneSameInstant(ZoneId.of("UTC"));
        }
    }

    static class DateConverter implements Converter<Date> {
        @Override
        public Date convert(final Literal value) {
            return convert(value.getLexicalForm());
        }

        public Date convert(final String value) {
            return Date.valueOf(value);
        }
    }

    static class TimeConverter implements Converter<Time> {
        @Override
        public Time convert(final Literal value) {
            return convert(value.getLexicalForm());
        }

        public Time convert(final String value) {
            return Time.valueOf(value);
        }
    }

}
