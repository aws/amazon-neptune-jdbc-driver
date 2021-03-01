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
 *
 */

package software.amazon.neptune.opencypher.resultset;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.sql.Statement;

public class OpenCypherEmptyResultSet  extends OpenCypherResultSetGetString {
    private static final ResultSetInfoWithoutRows RESULT_SET_INFO_WITHOUT_ROWS =
            new ResultSetInfoWithoutRows(null, null, 0, ImmutableList.of());

    /**
     * OpenCypherEmptyResultSet constructor, initializes super class.
     *
     * @param statement Statement Object.
     */
    public OpenCypherEmptyResultSet(final Statement statement) {
        super(statement, RESULT_SET_INFO_WITHOUT_ROWS, ImmutableList.of(ImmutableMap.of()));
    }
}
