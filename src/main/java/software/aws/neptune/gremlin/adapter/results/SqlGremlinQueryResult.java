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

package software.aws.neptune.gremlin.adapter.results;

import lombok.Getter;
import software.aws.neptune.gremlin.adapter.converter.SqlMetadata;

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

    public SqlGremlinQueryResult(final List<String> columns, final SqlMetadata sqlMetadata) throws SQLException {
        this.columns = columns;
        for (final String column : columns) {
            columnTypes.add(sqlMetadata.getType(column));
        }
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
