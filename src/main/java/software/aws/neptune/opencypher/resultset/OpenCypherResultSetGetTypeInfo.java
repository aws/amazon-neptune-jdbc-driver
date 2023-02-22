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

package software.aws.neptune.opencypher.resultset;

import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetTypeInfo;

import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OpenCypherResultSetGetTypeInfo extends ResultSetGetTypeInfo {
    private static final List<Map<String, Object>> TYPE_INFO = new ArrayList<>();

    static {
        // The order added to TYPE_INFO matters
        putInfo(TYPE_INFO, "BOOLEAN", Types.BIT, false, false);
        putInfo(TYPE_INFO, "NULL", Types.NULL, false, false);
        putInfo(TYPE_INFO, "INTEGER", Types.INTEGER, false, true);
        putInfo(TYPE_INFO, "NUMBER", Types.DOUBLE, false, true);
        putInfo(TYPE_INFO, "FLOAT", Types.DOUBLE, false, true);
        putInfo(TYPE_INFO, "STRING", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "ANY", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "LIST", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "MAP", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "NODE", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "RELATIONSHIP", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "PATH", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "POINT", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "DURATION", Types.VARCHAR, true, false);
        putInfo(TYPE_INFO, "BYTES", Types.VARCHAR, false, false);
        putInfo(TYPE_INFO, "DATE", Types.DATE, false, false);
        putInfo(TYPE_INFO, "TIME", Types.TIME, false, false);
        putInfo(TYPE_INFO, "LOCAL_TIME", Types.TIME, false, false);
        putInfo(TYPE_INFO, "DATE_TIME", Types.TIMESTAMP, false, false);
        putInfo(TYPE_INFO, "LOCAL_DATE_TIME", Types.TIMESTAMP, false, false);

        populateConstants(TYPE_INFO);
    }

    /**
     * OpenCypherResultSetGetTypeInfo constructor, initializes super class.
     *
     * @param statement                Statement Object.
     */
    public OpenCypherResultSetGetTypeInfo(final Statement statement) {
        super(statement, new ArrayList<>(TYPE_INFO));
    }
}
