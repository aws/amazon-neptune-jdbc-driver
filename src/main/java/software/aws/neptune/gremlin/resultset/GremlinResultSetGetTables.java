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

package software.aws.neptune.gremlin.resultset;

import software.aws.neptune.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import software.aws.neptune.common.ResultSetInfoWithoutRows;
import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetTables;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Gremlin ResultSet class for getTables.
 */
public class GremlinResultSetGetTables extends ResultSetGetTables implements java.sql.ResultSet {
    private static final Map<String, Class<?>> COLUMN_TYPE_MAP = new HashMap<>();
    // TODO getTables() JavaDoc description has less properties listed, should this reflect that?
    static {
        // TODO AN-577 move this stuff to common.
        COLUMN_TYPE_MAP.put("TABLE_CAT", String.class);
        COLUMN_TYPE_MAP.put("TABLE_SCHEM", String.class);
        COLUMN_TYPE_MAP.put("TABLE_NAME", String.class);
        COLUMN_TYPE_MAP.put("TABLE_TYPE", String.class);
        COLUMN_TYPE_MAP.put("REMARKS", String.class);
        COLUMN_TYPE_MAP.put("TYPE_CAT", String.class);
        COLUMN_TYPE_MAP.put("TYPE_SCHEM", String.class);
        COLUMN_TYPE_MAP.put("TYPE_NAME", String.class);
        COLUMN_TYPE_MAP.put("SELF_REFERENCING_COL_NAME", String.class);
        COLUMN_TYPE_MAP.put("REF_GENERATION", String.class);
    }

    /**
     * OpenCypherResultSetGetColumns constructor, initializes super class.
     *
     * @param statement                Statement Object.
     * @param gremlinSchema            GremlinSchema Object.
     * @param resultSetInfoWithoutRows ResultSetInfoWithoutRows Object.
     */
    public GremlinResultSetGetTables(final Statement statement,
                                     final GremlinSchema gremlinSchema,
                                     final ResultSetInfoWithoutRows resultSetInfoWithoutRows) {
        super(statement, gremlinSchema, resultSetInfoWithoutRows);
    }

    @Override
    protected ResultSetMetaData getResultMetadata() {
        final List<String> orderedColumns = getColumns();
        final List<Class<?>> rowTypes = new ArrayList<>();
        for (final String orderedColumn : orderedColumns) {
            rowTypes.add(COLUMN_TYPE_MAP.get(orderedColumn));
        }
        return new GremlinResultSetMetadata(orderedColumns, rowTypes);
    }
}
