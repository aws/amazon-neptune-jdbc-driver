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

package software.aws.neptune.common.gremlindatamodel.resultset;

import com.google.common.collect.ImmutableList;
import software.aws.neptune.common.ResultSetInfoWithoutRows;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base ResultSet for getTableTypes.
 */
public abstract class ResultSetGetTableTypes extends ResultSetGetString {
    /**
     * TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     */
    private static final List<String> COLUMNS = ImmutableList.of("TABLE_TYPE");
    private static final ResultSetInfoWithoutRows RESULT_SET_INFO_WITHOUT_ROWS =
            new ResultSetInfoWithoutRows(1, COLUMNS);
    private static final Map<String, String> CONVERSION_MAP = new HashMap<>();

    static {
        CONVERSION_MAP.put("TABLE_TYPE", "TABLE");
    }

    /**
     * ResultSetGetTableTypes constructor, initializes super class.
     *
     * @param statement Statement Object.
     */
    public ResultSetGetTableTypes(final Statement statement) {
        super(statement, RESULT_SET_INFO_WITHOUT_ROWS.getColumns(), RESULT_SET_INFO_WITHOUT_ROWS.getRowCount(),
                ImmutableList.of(CONVERSION_MAP));
    }

    public static List<String> getColumns() {
        return COLUMNS;
    }
}
