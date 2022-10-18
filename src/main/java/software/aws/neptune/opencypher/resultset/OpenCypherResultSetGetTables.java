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

package software.aws.neptune.opencypher.resultset;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;
import software.aws.neptune.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import software.aws.neptune.common.ResultSetInfoWithoutRows;
import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetTables;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OpenCypher ResultSet class for getTables.
 */
public class OpenCypherResultSetGetTables extends ResultSetGetTables implements java.sql.ResultSet {
    private static final Map<String, Type> COLUMN_TYPE_MAP = new HashMap<>();
    static {
        COLUMN_TYPE_MAP.put("TABLE_CAT", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("TABLE_SCHEM", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("TABLE_NAME", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("TABLE_TYPE", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("REMARKS", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("TYPE_CAT", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("TYPE_SCHEM", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("TYPE_NAME", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("SELF_REFERENCING_COL_NAME", InternalTypeSystem.TYPE_SYSTEM.STRING());
        COLUMN_TYPE_MAP.put("REF_GENERATION", InternalTypeSystem.TYPE_SYSTEM.STRING());
    }

    /**
     * OpenCypherResultSetGetColumns constructor, initializes super class.
     *
     * @param statement                Statement Object.
     * @param gremlinSchema            GremlinSchema Object.
     * @param resultSetInfoWithoutRows ResultSetInfoWithoutRows Object.
     */
    public OpenCypherResultSetGetTables(final Statement statement,
                                        final GremlinSchema gremlinSchema,
                                        final ResultSetInfoWithoutRows resultSetInfoWithoutRows)
            throws SQLException {
        super(statement, gremlinSchema, resultSetInfoWithoutRows);
    }

    @Override
    protected ResultSetMetaData getResultMetadata() {
        final List<String> orderedColumns = getColumns();
        final List<Type> rowTypes = new ArrayList<>();
        for (int i = 0; i < orderedColumns.size(); i++) {
            // TODO Didn't use the above map?
            rowTypes.add(InternalTypeSystem.TYPE_SYSTEM.STRING());
        }
        return new OpenCypherResultSetMetadata(orderedColumns, rowTypes);
    }
}
