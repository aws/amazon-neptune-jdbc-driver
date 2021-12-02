/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operands;

import org.apache.calcite.sql.SqlIdentifier;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import org.twilmes.sql.gremlin.adapter.util.SqlGremlinError;

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
            throw SqlGremlinError.get(SqlGremlinError.IDENTIFIER_INDEX_OUT_OF_BOUNDS);
        }
        return sqlIdentifier.names.get(idx);
    }


    public String getColumn() throws SQLException {
        if (sqlIdentifier.names.size() < 1) {
            throw SqlGremlinError.get(SqlGremlinError.IDENTIFIER_LIST_EMPTY);
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
