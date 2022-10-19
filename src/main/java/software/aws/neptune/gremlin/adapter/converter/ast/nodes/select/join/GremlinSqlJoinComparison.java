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
package software.aws.neptune.gremlin.adapter.converter.ast.nodes.select.join;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.gremlin.adapter.converter.SqlMetadata;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import software.aws.neptune.gremlin.adapter.util.SqlGremlinError;

import java.sql.SQLException;
import java.util.List;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlBinaryOperator in the context of a comparison of a JOIN.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class GremlinSqlJoinComparison {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlJoinComparison.class);
    // See SqlKind.BINARY_COMPARISON for list of aggregate functions in Calcite.

    private final SqlBasicCall sqlBasicCall;
    private final SqlBinaryOperator sqlBinaryOperator;
    private final SqlMetadata sqlMetadata;
    private final List<GremlinSqlNode> gremlinSqlNodes;


    public GremlinSqlJoinComparison(final SqlBasicCall sqlBasicCall,
                                    final SqlBinaryOperator sqlBinaryOperator,
                                    final List<GremlinSqlNode> gremlinSqlNodes,
                                    final SqlMetadata sqlMetadata) {
        this.sqlBasicCall = sqlBasicCall;
        this.sqlBinaryOperator = sqlBinaryOperator;
        this.sqlMetadata = sqlMetadata;
        this.gremlinSqlNodes = gremlinSqlNodes;
    }

    public boolean isEquals() {
        return sqlBinaryOperator.kind.sql.equals(SqlKind.EQUALS.sql);
    }

    public String getColumn(final String renamedTable) throws SQLException {
        for (final GremlinSqlNode gremlinSqlNode : gremlinSqlNodes) {
            if (!(gremlinSqlNode instanceof GremlinSqlIdentifier)) {
                throw SqlGremlinError.create(SqlGremlinError.UNEXPECTED_JOIN_NODES);
            }
            final GremlinSqlIdentifier gremlinSqlIdentifier = (GremlinSqlIdentifier) gremlinSqlNode;
            if (gremlinSqlIdentifier.getName(0).equals(renamedTable)) {
                return gremlinSqlIdentifier.getName(1);
            }
        }
        throw SqlGremlinError.create(SqlGremlinError.NO_JOIN_COLUMN);
    }
}
