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

package software.aws.neptune.gremlin.adapter.results.pagination;

import lombok.AllArgsConstructor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.gremlin.adapter.results.SqlGremlinQueryResult;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class Pagination implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Pagination.class);
    private static final int DEFAULT_PAGE_SIZE = 1000;
    private final int pageSize = DEFAULT_PAGE_SIZE;
    private final GetRowFromMap getRowFromMap;
    private final GraphTraversal<?, ?> traversal;
    private final SqlGremlinQueryResult sqlGremlinQueryResult;

    @Override
    public void run() {
        try {
            LOGGER.info("Graph traversal: " +
                    GroovyTranslator.of("g").translate(traversal.asAdmin().getBytecode()));
            while (traversal.hasNext()) {
                final List<Object> rows = new ArrayList<>();
                traversal.next(pageSize).forEach(map -> {
                    // Our choose(<predicate>, <expected>, <empty>) returns an empty list.
                    // If we get that, we just want to skip over it.
                    if (map instanceof Map) {
                        rows.add(getRowFromMap.execute((Map<String, Object>) map));
                    }
                });
                convertAndInsertResult(sqlGremlinQueryResult, rows);
            }
            // If we run out of traversal data (or hit our limit), stop and signal to the result that it is done.
            sqlGremlinQueryResult.close();
        } catch (final Exception e) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            LOGGER.error("Encountered exception", e);
            sqlGremlinQueryResult.setPaginationException(new SQLException(e + sw.toString()));
        } finally {
            closeTraversal();
        }
    }

    void closeTraversal() {
        try {
            traversal.close();
        } catch (final Exception ignored) {
        }
    }

    /**
     * converts input row results and insert them into sqlGremlinQueryResult
     */
    void convertAndInsertResult(final SqlGremlinQueryResult sqlGremlinQueryResult, final List<Object> rows) {
        final List<List<Object>> finalRowResult = new ArrayList<>();
        for (final Object row : rows) {
            final List<Object> convertedRow = new ArrayList<>();
            if (row instanceof Object[]) {
                convertedRow.addAll(Arrays.asList((Object[]) row));
            } else {
                convertedRow.add(row);
            }
            finalRowResult.add(convertedRow);
        }
        sqlGremlinQueryResult.addResults(finalRowResult);
    }
}
