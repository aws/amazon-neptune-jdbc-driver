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

package software.amazon.neptune.sparql.mock;

public class SparqlMockDataQuery {
    public static final String STRING_QUERY =
            "SELECT ?x ?fname WHERE {?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  ?fname}";
    public static final String BOOL_QUERY = "SELECT ?x ?bool WHERE {?x  <http://somewhere/peopleInfo#bool>  ?bool}";
    public static final String BYTE_QUERY = "SELECT ?x ?byte WHERE {?x  <http://somewhere/peopleInfo#byte>  ?byte}";
    public static final String SHORT_QUERY = "SELECT ?x ?short WHERE {?x  <http://somewhere/peopleInfo#short>  ?short}";
    public static final String INTEGER_SMALL_QUERY =
            "SELECT ?x ?integerSm WHERE {?x  <http://somewhere/peopleInfo#integerSm>  ?integerSm}";
    public static final String INTEGER_LARGE_QUERY =
            "SELECT ?x ?integerLg WHERE {?x  <http://somewhere/peopleInfo#integerLg>  ?integerLg}";
    public static final String LONG_QUERY = "SELECT ?x ?long WHERE {?x  <http://somewhere/peopleInfo#long>  ?long}";
    public static final String INT_QUERY = "SELECT ?x ?int WHERE {?x  <http://somewhere/peopleInfo#int>  ?int}";
    public static final String DECIMAL_QUERY =
            "SELECT ?x ?decimal WHERE {?x  <http://somewhere/peopleInfo#decimal>  ?decimal}";
    public static final String DOUBLE_QUERY =
            "SELECT ?x ?double WHERE {?x  <http://somewhere/peopleInfo#double>  ?double}";
    public static final String FLOAT_QUERY = "SELECT ?x ?float WHERE {?x  <http://somewhere/peopleInfo#float>  ?float}";
    // NOTE: all unsigned XSD classes are wrapped into Java Integer or Long depending on size by the Jena library
    public static final String UNSIGNED_BYTE_QUERY =
            "SELECT ?x ?unsignedByte WHERE {?x  <http://somewhere/peopleInfo#unsignedByte>  ?unsignedByte}";
    public static final String UNSIGNED_SHORT_QUERY =
            "SELECT ?x ?unsignedShort WHERE {?x  <http://somewhere/peopleInfo#unsignedShort>  ?unsignedShort}";
    public static final String UNSIGNED_INT_QUERY =
            "SELECT ?x ?unsignedInt WHERE {?x  <http://somewhere/peopleInfo#unsignedInt>  ?unsignedInt}";
    public static final String UNSIGNED_LONG_SMALL_QUERY =
            "SELECT ?x ?unsignedLongSm WHERE {?x  <http://somewhere/peopleInfo#unsignedLongSm>  ?unsignedLongSm}";
    public static final String UNSIGNED_LONG_LARGE_QUERY =
            "SELECT ?x ?unsignedLongLg WHERE {?x  <http://somewhere/peopleInfo#unsignedLongLg>  ?unsignedLongLg}";
    public static final String POSITIVE_INTEGER_QUERY =
            "SELECT ?x ?positiveInteger WHERE {?x  <http://somewhere/peopleInfo#positiveInteger>  ?positiveInteger}";
    public static final String NON_NEGATIVE_INTEGER_QUERY =
            "SELECT ?x ?nonNegativeInteger WHERE {?x  <http://somewhere/peopleInfo#nonNegativeInteger>  ?nonNegativeInteger}";
    public static final String NEGATIVE_INTEGER_QUERY =
            "SELECT ?x ?negativeInteger WHERE {?x  <http://somewhere/peopleInfo#negativeInteger>  ?negativeInteger}";
    public static final String NON_POSITIVE_INTEGER_QUERY =
            "SELECT ?x ?nonPositiveInteger WHERE {?x  <http://somewhere/peopleInfo#nonPositiveInteger>  ?nonPositiveInteger}";
    public static final String DATE_QUERY = "SELECT ?x ?date WHERE {?x  <http://somewhere/peopleInfo#date>  ?date}";
    public static final String TIME_QUERY = "SELECT ?x ?time WHERE {?x  <http://somewhere/peopleInfo#time>  ?time}";
    public static final String DATE_TIME_QUERY =
            "SELECT ?x ?dateTime WHERE {?x  <http://somewhere/peopleInfo#dateTime>  ?dateTime}";
    public static final String DATE_TIME_STAMP_QUERY =
            "SELECT ?x ?dateTimeStamp WHERE {?x  <http://somewhere/peopleInfo#dateTimeStamp>  ?dateTimeStamp}";
    public static final String G_YEAR_QUERY =
            "SELECT ?x ?gYear WHERE {?x  <http://somewhere/peopleInfo#gYear>  ?gYear}";
    public static final String G_MONTH_QUERY =
            "SELECT ?x ?gMonth WHERE {?x  <http://somewhere/peopleInfo#gMonth>  ?gMonth}";
    public static final String G_DAY_QUERY = "SELECT ?x ?gDay WHERE {?x  <http://somewhere/peopleInfo#gDay>  ?gDay}";
    public static final String G_YEAR_MONTH_QUERY =
            "SELECT ?x ?gYearMonth WHERE {?x  <http://somewhere/peopleInfo#gYearMonth>  ?gYearMonth}";
    public static final String G_MONTH_DAY_QUERY =
            "SELECT ?x ?gMonthDay WHERE {?x  <http://somewhere/peopleInfo#gMonthDay>  ?gMonthDay}";
    public static final String DURATION_QUERY =
            "SELECT ?x ?duration WHERE {?x  <http://somewhere/peopleInfo#duration>  ?duration}";
    public static final String YEAR_MONTH_DURATION_QUERY =
            "SELECT ?x ?yearMonthDuration WHERE {?x  <http://somewhere/peopleInfo#yearMonthDuration>  ?yearMonthDuration}";
    public static final String DAY_TIME_DURATION_QUERY =
            "SELECT ?x ?dayTimeDuration WHERE {?x  <http://somewhere/peopleInfo#dayTimeDuration>  ?dayTimeDuration}";
    public static final String PREDICATE_QUERY =
            "SELECT ?fname ?x WHERE {?x  <http://www.w3.org/2001/vcard-rdf/3.0#FN>  ?fname}";
}
