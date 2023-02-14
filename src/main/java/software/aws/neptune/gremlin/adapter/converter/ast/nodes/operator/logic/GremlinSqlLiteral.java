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

package software.aws.neptune.gremlin.adapter.converter.ast.nodes.operator.logic;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import software.aws.neptune.gremlin.adapter.converter.SqlMetadata;
import software.aws.neptune.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;

import java.sql.SQLException;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlLiteral.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class GremlinSqlLiteral extends GremlinSqlNode {
    final SqlLiteral sqlLiteral;
    public GremlinSqlLiteral(final SqlLiteral sqlLiteral,
                             final SqlMetadata sqlMetadata) {
        super(sqlLiteral, sqlMetadata);
        this.sqlLiteral = sqlLiteral;
    }

    public void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        graphTraversal.constant(getValue());
    }

    public Object getValue() {
        return (sqlLiteral.getTypeName().equals(SqlTypeName.CHAR)) ? sqlLiteral.toValue() : sqlLiteral.getValue();
    }
}
