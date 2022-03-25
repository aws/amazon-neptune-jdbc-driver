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

package software.aws.neptune.common.gremlindatamodel.resultset;

import com.google.common.collect.ImmutableList;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: Things extending this should switch to dependency injection to reduce the number of classes that have the same code.

/**
 * Base ResultSet for getCatalogs.
 */
public abstract class ResultSetGetCatalogs extends ResultSetGetString {
    /**
     * TABLE_CAT String => catalog name
     */
    private static final List<String> COLUMNS = ImmutableList.of("TABLE_CAT");
    private static final Map<String, String> CONVERSION_MAP = new HashMap<>();

    static {
        CONVERSION_MAP.put("TABLE_CAT", null);
    }

    /**
     * ResultSetGetCatalogs constructor, initializes super class.
     *
     * @param statement Statement Object.
     */
    public ResultSetGetCatalogs(final Statement statement) {
        super(statement, ImmutableList.of("TABLE_CAT"), 0, ImmutableList.of(CONVERSION_MAP));
    }

    protected List<String> getColumns() {
        return COLUMNS;
    }
}
