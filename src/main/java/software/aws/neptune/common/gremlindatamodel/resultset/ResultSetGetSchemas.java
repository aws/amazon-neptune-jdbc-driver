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

    static {
        CONVERSION_MAP.put("TABLE_SCHEM", "gremlin");
        CONVERSION_MAP.put("TABLE_CAT", null);
    }

    /**
     * ResultSetGetSchemas constructor, initializes super class.
     *
     * @param statement Statement Object.
     */
    public ResultSetGetSchemas(final Statement statement) {
        super(statement, ImmutableList.of("TABLE_SCHEM", "TABLE_CAT"), 1,
                ImmutableList.of(CONVERSION_MAP));
    }

    protected List<String> getColumns() {
        return COLUMNS;
    }
}
