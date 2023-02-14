/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.gremlin.adapter.graphs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GraphConstants {
    public static final String STRING_VALUE = "value";
    public static final Long LONG_VALUE = 100L;
    public static final Integer INTEGER_VALUE = LONG_VALUE.intValue();
    public static final Short SHORT_VALUE = LONG_VALUE.shortValue();
    public static final Byte BYTE_VALUE = LONG_VALUE.byteValue();
    public static final Double DOUBLE_VALUE = LONG_VALUE.doubleValue();
    public static final Float FLOAT_VALUE = LONG_VALUE.floatValue();
    public static final Date DATE_VALUE;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-mm-dd");

    static {
        Date date = null;
        try {
            date = DATE_FORMATTER.parse("1993-03-30");
        } catch (final ParseException e) {
            e.printStackTrace();
        }
        DATE_VALUE = date;
    }
}
