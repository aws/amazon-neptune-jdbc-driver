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

package software.aws.neptune.gremlin.adapter.converter.ast.nodes.operands;

import org.apache.calcite.sql.SqlIdentifier;
import software.aws.neptune.gremlin.adapter.converter.SqlMetadata;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import software.aws.neptune.gremlin.adapter.util.SqlGremlinError;

import java.sql.SQLException;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlIdentifier.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class GremlinSqlIdentifier extends GremlinSqlNode {
    private final SqlIdentifier sqlIdentifier;

    public GremlinSqlIdentifier(final SqlIdentifier sqlIdentifier, final SqlMetadata sqlMetadata) {
        super(sqlIdentifier, sqlMetadata);
        this.sqlIdentifier = sqlIdentifier;
    }


    public String getName(final int idx) throws SQLException {
        if (idx >= sqlIdentifier.names.size()) {
            throw SqlGremlinError.create(SqlGremlinError.IDENTIFIER_INDEX_OUT_OF_BOUNDS);
        }
        return sqlIdentifier.names.get(idx);
    }


    public String getColumn() throws SQLException {
        if (sqlIdentifier.names.size() < 1) {
            throw SqlGremlinError.create(SqlGremlinError.IDENTIFIER_LIST_EMPTY);
        }
        return sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
    }

    public int getNameCount() {
        return sqlIdentifier.names.size();
    }

    public boolean isStar() {
        return sqlIdentifier.isStar();
    }
}
