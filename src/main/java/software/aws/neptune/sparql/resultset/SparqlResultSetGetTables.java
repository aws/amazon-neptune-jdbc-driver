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

import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import software.aws.neptune.common.ResultSetInfoWithoutRows;
import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetTables;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparqlResultSetGetTables extends ResultSetGetTables implements java.sql.ResultSet {
    private static final Map<String, Class<?>> TABLE_TYPE_MAP = new HashMap<>();

    // REF https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getTables
    static {
        TABLE_TYPE_MAP.put("TABLE_CAT", String.class);
        TABLE_TYPE_MAP.put("TABLE_SCHEM", String.class);
        TABLE_TYPE_MAP.put("TABLE_NAME", String.class);
        TABLE_TYPE_MAP.put("TABLE_TYPE", String.class);
        TABLE_TYPE_MAP.put("REMARKS", String.class);
        TABLE_TYPE_MAP.put("TYPE_CAT", String.class);
        TABLE_TYPE_MAP.put("TYPE_SCHEM", String.class);
        TABLE_TYPE_MAP.put("TYPE_NAME", String.class);
        TABLE_TYPE_MAP.put("SELF_REFERENCING_COL_NAME", String.class);
        TABLE_TYPE_MAP.put("REF_GENERATION", String.class);
    }

    /**
     * ResultSetGetTables constructor, initializes super class.
     *
     * @param statement                Statement Object.
     * @param gremlinSchema            GremlinSchema Object.
     * @param resultSetInfoWithoutRows ResultSetInfoWithoutRows Object.
     */
    public SparqlResultSetGetTables(final Statement statement,
                                    final GremlinSchema gremlinSchema,
                                    final ResultSetInfoWithoutRows resultSetInfoWithoutRows) {
        super(statement, gremlinSchema, resultSetInfoWithoutRows);
    }

    @Override
    protected ResultSetMetaData getResultMetadata() throws SQLException {
        final List<String> orderedColumns = getColumns();
        final List<Object> rowTypes = new ArrayList<>();
        for (final String column : orderedColumns) {
            rowTypes.add(TABLE_TYPE_MAP.get(column));
        }
        return new SparqlResultSetMetadata(getColumns(), rowTypes);
    }
}
