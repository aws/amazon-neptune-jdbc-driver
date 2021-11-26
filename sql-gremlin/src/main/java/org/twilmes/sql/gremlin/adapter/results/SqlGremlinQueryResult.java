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

package org.twilmes.sql.gremlin.adapter.results;

import lombok.Getter;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinTableBase;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class SqlGremlinQueryResult implements AutoCloseable {
    public static final String EMPTY_MESSAGE = "No more results.";
    public static final String NULL_VALUE = "$%#NULL#%$";
    private final List<String> columns;
    private final List<String> columnTypes = new ArrayList<>();
    private final BlockingQueue<List<Object>> blockingQueueRows = new LinkedBlockingQueue<>();
    private SQLException paginationException = null;

    public SqlGremlinQueryResult(final List<String> columns, final List<GremlinTableBase> gremlinTableBases,
                                 final SqlMetadata sqlMetadata) throws SQLException {
        this.columns = columns;
        for (final String column : columns) {
            columnTypes.add(getType(column, sqlMetadata, gremlinTableBases));
        }
    }

    private String getType(final String column, final SqlMetadata sqlMetadata,
                           final List<GremlinTableBase> gremlinTableBases) throws SQLException {
        if (sqlMetadata.aggregateTypeExists(column)) {
            return sqlMetadata.getOutputType(column, "string");
        }
        String renamedColumn = sqlMetadata.getRenamedColumn(column);
        if (!sqlMetadata.aggregateTypeExists(renamedColumn)) {
            // Sometimes columns are double renamed.
            renamedColumn = sqlMetadata.getRenamedColumn(renamedColumn);
            for (final GremlinTableBase gremlinTableBase : gremlinTableBases) {
                if (sqlMetadata.getTableHasColumn(gremlinTableBase, renamedColumn)) {
                    return sqlMetadata.getGremlinProperty(gremlinTableBase.getLabel(), renamedColumn).getType();
                }
            }
        }
        return sqlMetadata.getOutputType(renamedColumn, "string");
    }

    public void setPaginationException(final SQLException e) {
        paginationException = e;
        close();
    }

    @Override
    public void close() {
        blockingQueueRows.add(new EmptyResult());
    }

    public void addResults(final List<List<Object>> rows) {
        // This is a workaround for Gremlin null support not being in any version of Gremlin that is
        // widely supported by database vendors.
        rows.forEach(row -> row.replaceAll(col -> (col instanceof String && col.equals(NULL_VALUE) ? null : col)));
        blockingQueueRows.addAll(rows);
    }

    public List<Object> getResult() throws SQLException {
        while (true) {
            try {
                final List<Object> result = blockingQueueRows.take();

                // If a pagination exception occurs, an EmptyResult Object will be inserted into the BlockingQueue.
                // The pagination exception needs to be checked before returning.
                if (paginationException != null) {
                    throw paginationException;
                }
                return result;
            } catch (final InterruptedException ignored) {
            }
        }
    }

    public static class EmptyResult extends ArrayList<Object> {
    }
}
