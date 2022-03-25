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

package software.aws.neptune.sparql.resultset;

import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetTypeInfo;

import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparqlResultSetGetTypeInfo extends ResultSetGetTypeInfo {
    private static final List<Map<String, Object>> TYPE_INFO = new ArrayList<>();

    static {
        // The order added to TYPE_INFO matters
        putInfo(TYPE_INFO, "XSDboolean", Types.BIT, false, false);
        putInfo(TYPE_INFO, "XSDbyte", Types.TINYINT, false, true);
        putInfo(TYPE_INFO, "XSDinteger", Types.BIGINT, false, true);
        putInfo(TYPE_INFO, "XSDnonPositiveInteger", Types.BIGINT, false, true);
        putInfo(TYPE_INFO, "XSDnonNegativeInteger", Types.BIGINT, false, true);
        putInfo(TYPE_INFO, "XSDpositiveInteger", Types.BIGINT, false, true);
        putInfo(TYPE_INFO, "XSDnegativeInteger", Types.BIGINT, false, true);
        putInfo(TYPE_INFO, "XSDunsignedInt", Types.BIGINT, false, true, true);
        putInfo(TYPE_INFO, "XSDlong", Types.BIGINT, false, true);
        putInfo(TYPE_INFO, "XSDunsignedLong", Types.BIGINT, false, true, true);
        putInfo(TYPE_INFO, "XSDdecimal", Types.DECIMAL, false, true);
        putInfo(TYPE_INFO, "XSDint", Types.INTEGER, false, true);
        putInfo(TYPE_INFO, "XSDunsignedByte", Types.INTEGER, false, true, true);
        putInfo(TYPE_INFO, "XSDunsignedShort", Types.INTEGER, false, true, true);
        putInfo(TYPE_INFO, "XSDshort", Types.SMALLINT, false, true);
        putInfo(TYPE_INFO, "XSDfloat", Types.REAL, false, true);
        putInfo(TYPE_INFO, "XSDdouble", Types.DOUBLE, false, true);
        putInfo(TYPE_INFO, "XSDstring", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "XSDduration", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "XSDdate", Types.DATE, false, false);
        putInfo(TYPE_INFO, "XSDtime", Types.TIME, false, false);
        putInfo(TYPE_INFO, "XSDdateTimeStamp", Types.TIMESTAMP, false, false);
        putInfo(TYPE_INFO, "XSDdateTime", Types.TIMESTAMP, false, false);

        populateConstants(TYPE_INFO);
    }

    /**
     * SparqlResultSetGetTypeInfo constructor, initializes super class.
     *
     * @param statement                Statement Object.
     */
    public SparqlResultSetGetTypeInfo(final Statement statement) {
        super(statement, new ArrayList<>(TYPE_INFO));
    }
}
