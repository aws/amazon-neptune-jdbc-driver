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

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
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
    public static final Map<XSDDatatype, Class<?>> SPARQL_LITERAL_TO_JAVA_TYPE_MAP = new HashMap<>();
    public static final Map<XSDDatatype, JdbcType> SPARQL_LITERAL_TO_JDBC_TYPE_MAP = new HashMap<>();
    public static final Map<XSDDatatype, SparqlTypeMapping.Converter<?>> SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP =
            new HashMap<>();
    public static final Converter<Timestamp> DATE_TIME_CONVERTER = new DateTimeConverter();
    public static final Converter<ZonedDateTime> DATE_TIME_STAMP_CONVERTER = new DateTimeStampConverter();
    public static final Converter<Date> DATE_CONVERTER = new DateConverter();
    public static final Converter<Time> TIME_CONVERTER = new TimeConverter();


    static {
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDdecimal, java.math.BigDecimal.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDinteger, java.math.BigInteger.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDnonPositiveInteger, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDnonNegativeInteger, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDpositiveInteger, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDnegativeInteger, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDbyte, Byte.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDunsignedByte, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDdouble, Double.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDfloat, Float.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDlong, Long.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDunsignedInt, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDunsignedShort, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDunsignedLong, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDint, Integer.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDshort, Short.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDboolean, Boolean.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDbase64Binary, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDhexBinary, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDdate, java.sql.Date.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDtime, java.sql.Time.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDdateTime, java.sql.Timestamp.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDdateTimeStamp, java.sql.Timestamp.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDduration, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDyearMonthDuration, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDdayTimeDuration, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDgYearMonth, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDgMonthDay, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDgMonth, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDgDay, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDgYear, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDnormalizedString, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDstring, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDanyURI, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDtoken, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDName, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDlanguage, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDQName, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDNMTOKEN, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDID, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDENTITY, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDNCName, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDNOTATION, String.class);
        SPARQL_LITERAL_TO_JAVA_TYPE_MAP.put(XSDDatatype.XSDIDREF, String.class);

        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDtime, TIME_CONVERTER);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDdate, DATE_CONVERTER);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDdateTime, DATE_TIME_CONVERTER);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDdateTimeStamp, DATE_TIME_STAMP_CONVERTER);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDbase64Binary, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDhexBinary, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDduration, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDyearMonthDuration, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDdayTimeDuration, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDgYearMonth, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDgMonthDay, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDgMonth, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDgDay, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDgYear, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDnormalizedString, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDstring, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDanyURI, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDtoken, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDName, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDlanguage, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDQName, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDNMTOKEN, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDID, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDENTITY, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDNCName, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDNOTATION, Literal::getLexicalForm);
        SPARQL_LITERAL_TO_JAVA_TRANSFORM_MAP.put(XSDDatatype.XSDIDREF, Literal::getLexicalForm);
        // NOTE: Gregorian date types are not supported currently due to incompatibility with java and JDBC datatype,
        // currently returning as String

        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDdecimal, JdbcType.DECIMAL);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDinteger, JdbcType.BIGINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDnonPositiveInteger, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDnonNegativeInteger, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDpositiveInteger, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDnegativeInteger, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDbyte, JdbcType.TINYINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDunsignedByte, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDdouble, JdbcType.DOUBLE);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDfloat, JdbcType.REAL);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDlong, JdbcType.BIGINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDunsignedInt, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDunsignedShort, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDunsignedLong, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDint, JdbcType.INTEGER);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDshort, JdbcType.SMALLINT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDboolean, JdbcType.BIT);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDbase64Binary, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDhexBinary, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDdate, JdbcType.DATE);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDtime, JdbcType.TIME);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDdateTime, JdbcType.TIMESTAMP);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDdateTimeStamp, JdbcType.TIMESTAMP);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDduration, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDyearMonthDuration, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDdayTimeDuration, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDgYearMonth, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDgMonthDay, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDgMonth, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDgDay, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDgYear, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDnormalizedString, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDstring, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDanyURI, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDtoken, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDName, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDlanguage, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDQName, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDNMTOKEN, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDID, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDENTITY, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDNCName, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDNOTATION, JdbcType.VARCHAR);
        SPARQL_LITERAL_TO_JDBC_TYPE_MAP.put(XSDDatatype.XSDIDREF, JdbcType.VARCHAR);

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
    }

    /**
     * Function to get JDBC type equivalent of Sparql input type.
     *
     * @param sparqlClass Sparql Datatype.
     * @return JDBC equivalent for Sparql class type.
     */
    public static JdbcType getJDBCType(final Object sparqlClass) {
        // TODO catching cast exception?
        return sparqlClass instanceof RDFDatatype ?
                SPARQL_LITERAL_TO_JDBC_TYPE_MAP.getOrDefault((XSDDatatype) sparqlClass, JdbcType.VARCHAR) :
                JdbcType.VARCHAR;
    }

    /**
     * Function to get Java type equivalent of Sparql input type.
     *
     * @param sparqlClass Sparql Datatype.
     * @return Java equivalent for Sparql class type.
     */
    public static Class<?> getJavaType(final Object sparqlClass) {
        // TODO catching cast exception?
        return sparqlClass instanceof RDFDatatype ?
                SPARQL_LITERAL_TO_JAVA_TYPE_MAP.getOrDefault((XSDDatatype) sparqlClass, String.class) :
                String.class;
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
