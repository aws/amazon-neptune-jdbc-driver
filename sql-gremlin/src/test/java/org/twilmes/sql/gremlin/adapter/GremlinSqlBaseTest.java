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

package org.twilmes.sql.gremlin.adapter;

import lombok.Getter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.jupiter.api.Assertions;
import org.twilmes.sql.gremlin.adapter.converter.SqlConverter;
import org.twilmes.sql.gremlin.adapter.converter.schema.SqlSchemaGrabber;
import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import org.twilmes.sql.gremlin.adapter.graphs.TestGraphFactory;
import org.twilmes.sql.gremlin.adapter.results.SqlGremlinQueryResult;
import org.twilmes.sql.gremlin.adapter.util.SQLNotSupportedException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by twilmes on 12/4/15.
 */
public abstract class GremlinSqlBaseTest {
    private final Graph graph;
    private final GraphTraversalSource g;
    private final SqlConverter converter;

    GremlinSqlBaseTest() throws SQLException {
        graph = TestGraphFactory.createGraph(getDataSet());
        g = graph.traversal();
        final GremlinSchema gremlinSchema = SqlSchemaGrabber.getSchema(g, SqlSchemaGrabber.ScanType.All);
        converter = new SqlConverter(gremlinSchema);
    }

    protected abstract DataSet getDataSet();

    protected void runQueryTestColumnType(final String query) throws SQLException {
        final SqlGremlinQueryResult sqlGremlinQueryResult = converter.executeQuery(g, query);
        final int columnCount = sqlGremlinQueryResult.getColumns().size();
        final SqlGremlinTestResult result = new SqlGremlinTestResult(sqlGremlinQueryResult);
        final List<Class<?>> returnedColumnType = new ArrayList<>();
        final List<Class<?>> metadataColumnType = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            returnedColumnType.add(result.getRows().get(0).get(i).getClass());
            metadataColumnType.add(getType(sqlGremlinQueryResult.getColumnTypes().get(i)));
        }

        assertResultTypes(returnedColumnType, metadataColumnType);
    }

    public void assertResultTypes(final List<Class<?>> actual, final List<Class<?>> expected) {
        Assertions.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            Assertions.assertEquals(expected.get(i), actual.get(i));
        }
    }

    private Class<?> getType(final String className) {
        if ("string".equalsIgnoreCase(className)) {
            return String.class;
        } else if ("integer".equalsIgnoreCase(className)) {
            return Integer.class;
        } else if ("float".equalsIgnoreCase(className)) {
            return Float.class;
        } else if ("byte".equalsIgnoreCase(className)) {
            return Byte.class;
        } else if ("short".equalsIgnoreCase(className)) {
            return Short.class;
        } else if ("double".equalsIgnoreCase(className)) {
            return Double.class;
        } else if ("long".equalsIgnoreCase(className)) {
            return Long.class;
        } else if ("boolean".equalsIgnoreCase(className)) {
            return Boolean.class;
        } else if ("date".equalsIgnoreCase(className) || "long_date".equalsIgnoreCase(className)) {
            return java.sql.Date.class;
        } else if ("timestamp".equalsIgnoreCase(className) || "long_timestamp".equalsIgnoreCase(className)) {
            return java.sql.Timestamp.class;
        } else {
            return null;
        }
    }

    protected void runQueryTestResults(final String query, final List<String> columnNames,
                                       final List<List<?>> rows)
            throws SQLException {
        final SqlGremlinTestResult result = new SqlGremlinTestResult(converter.executeQuery(g, query));
        assertRows(result.getRows(), rows);
        assertColumns(result.getColumns(), columnNames);
    }

    protected void runQueryTestThrows(final String query, final String errorMessage) {
        Assertions.assertThrows(SQLException.class, () -> converter.executeQuery(g, query), errorMessage);
    }

    protected void runNotSupportedQueryTestThrows(final String query, final String errorMessage) throws SQLException {
        Assertions.assertThrows(SQLNotSupportedException.class, () -> converter.executeQuery(g, query), errorMessage);
    }

    @SafeVarargs
    public final List<List<?>> rows(final List<Object>... rows) {
        return new ArrayList<>(Arrays.asList(rows));
    }

    public List<String> columns(final String... columns) {
        return new ArrayList<>(Arrays.asList(columns));
    }

    public List<Object> r(final Object... row) {
        return new ArrayList<>(Arrays.asList(row));
    }

    public void assertRows(final List<List<?>> actual, final List<List<?>> expected) {
        Assertions.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            Assertions.assertEquals(expected.get(i).size(), actual.get(i).size());
            for (int j = 0; j < actual.get(i).size(); j++) {
                Assertions.assertEquals(expected.get(i).get(j), actual.get(i).get(j));
            }
        }
    }

    public void assertColumns(final List<String> actual, final List<String> expected) {
        Assertions.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            Assertions.assertEquals(expected.get(i), actual.get(i));
        }
    }

    public enum DataSet {
        SPACE,
        DATA_TYPES
    }

    @Getter
    static class SqlGremlinTestResult {
        private final List<List<?>> rows = new ArrayList<>();
        private final List<String> columns;

        SqlGremlinTestResult(final SqlGremlinQueryResult sqlGremlinQueryResult) throws SQLException {
            columns = sqlGremlinQueryResult.getColumns();
            List<?> res;
            do {
                res = sqlGremlinQueryResult.getResult();
                if (!(res instanceof SqlGremlinQueryResult.EmptyResult)) {
                    this.rows.add(res);
                }
            } while (!(res instanceof SqlGremlinQueryResult.EmptyResult));
        }
    }
}
