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

package software.amazon.neptune.common.gremlindatamodel.resultset;

import com.google.common.collect.ImmutableList;
import software.amazon.neptune.common.ResultSetInfoWithoutRows;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base ResultSet for getSchemas.
 */
public abstract class ResultSetGetSchemas extends ResultSetGetString {
    /**
     * TABLE_CAT String => catalog name
     */
    private static final List<String> COLUMNS = ImmutableList.of("TABLE_SCHEM", "TABLE_CAT");
    private static final Map<String, String> CONVERSION_MAP = new HashMap<>();
    private static final ResultSetInfoWithoutRows RESULT_SET_INFO_WITHOUT_ROWS =
            new ResultSetInfoWithoutRows(0, COLUMNS);

    static {
        CONVERSION_MAP.put("TABLE_SCHEM", null);
        CONVERSION_MAP.put("TABLE_CAT", null);
    }

    /**
     * ResultSetGetSchemas constructor, initializes super class.
     *
     * @param statement Statement Object.
     */
    public ResultSetGetSchemas(final Statement statement) {
        super(statement, RESULT_SET_INFO_WITHOUT_ROWS.getColumns(), RESULT_SET_INFO_WITHOUT_ROWS.getRowCount(),
                ImmutableList.of(CONVERSION_MAP));
    }

    protected List<String> getColumns() {
        return COLUMNS;
    }
}
