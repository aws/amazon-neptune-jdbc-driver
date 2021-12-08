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

import org.junit.jupiter.api.Test;
import java.sql.SQLException;

public class GremlinSqlAggregateTest extends GremlinSqlBaseTest {

    GremlinSqlAggregateTest() throws SQLException {
    }

    @Override
    protected DataSet getDataSet() {
        return DataSet.SPACE;
    }

    @Test
    public void testAggregateFunctions() throws SQLException {
        runQueryTestResults("select count(age), min(age), max(age), avg(age) from person",
                columns("COUNT(age)", "MIN(age)", "MAX(age)", "AVG(age)"),
                rows(r(6L, 29, 50, 36.666666666666664)));
    }

    @Test
    public void testAggregateColumnTypeNoRename() throws SQLException {
        // validate the metadata type matches return type
        runQueryTestColumnType("select count(age), min(age), max(age), avg(age) from person");
    }

    @Test
    public void testAggregateColumnTypeOneRename() throws SQLException {
        // validate the metadata type matches return type
        runQueryTestColumnType("select count(age) as c from person");
    }

    @Test
    public void testAggregateColumnTypeAllRename() throws SQLException {
        // validate the metadata type matches return type
        runQueryTestColumnType("select count(age) as c, min(age) as m1, max(age) as m2, avg(age) as a from person");
    }

    @Test
    public void testAggregateColumnTypeMixedRename() throws SQLException {
        // validate the metadata type matches return type
        runQueryTestColumnType("select count(age) as c, min(age) as m1, max(age), avg(age) from person");
    }

    @Test
    public void testAggregateColumnTypeMixedAgg() throws SQLException {
        // validate the metadata type matches return type
        runQueryTestColumnType("select age, count(age) from person group by age");
    }

    @Test
    public void testCountStar() throws SQLException {
        // Validate that the output column is COUNT(*) and the value is correct.
        runQueryTestResults("SELECT COUNT(*) FROM person", columns("COUNT(*)"), rows(r(6L)));
    }

    @Test
    public void testCountWhereGroupBy() throws SQLException {
        runQueryTestResults("SELECT wentToSpace, COUNT(age) FROM person WHERE age > 30 GROUP BY wentToSpace",
                columns("wentToSpace", "COUNT(age)"), rows(r(false, 2L), r(true, 2L)));
        runQueryTestResults("SELECT wentToSpace, COUNT(age) FROM person WHERE age > 50 GROUP BY wentToSpace",
                columns("wentToSpace", "COUNT(age)"), rows());
        runQueryTestResults("SELECT wentToSpace, COUNT(age) FROM person WHERE age > 0 GROUP BY wentToSpace",
                columns("wentToSpace", "COUNT(age)"), rows(r(false, 3L), r(true, 3L)));
        runQueryTestResults("SELECT wentToSpace, COUNT(age) FROM person WHERE age < 100 AND wentToSpace = FALSE GROUP BY wentToSpace",
                columns("wentToSpace", "COUNT(age)"), rows(r(false, 3L)));
        runQueryTestResults("SELECT wentToSpace, COUNT(age) FROM person WHERE age > 31 AND wentToSpace = FALSE GROUP BY wentToSpace",
                columns("wentToSpace", "COUNT(age)"), rows(r(false, 1L)));
    }

    @Test
    public void testCastError() throws SQLException {
        // Validate that the output column is COUNT(*) and the value is correct.
        runQueryTestResults("SELECT CAST(17 AS varchar)", columns("perseon.age"), rows(r(6L)));
        runQueryTestResults("SELECT CAST(person.age as CHAR) FROM person", columns("perseon.age"), rows(r(6L)));
    }
}
