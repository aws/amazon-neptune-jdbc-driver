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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class GremlinSqlIncompleteSelectTest extends GremlinSqlBaseTest {

    GremlinSqlIncompleteSelectTest() throws SQLException {
    }

    @Override
    protected DataSet getDataSet() {
        return DataSet.SPACE_INCOMPLETE;
    }

    @Test
    public void testProject() throws SQLException {
        runQueryTestResults("select name from person", columns("name"),
                rows(r("Tom"), r("Patty"), r("Phil"), r("Susan"), r("Juanita"), r("Pavel")));
    }

    @Test
    public void testEdges() throws SQLException {
        runQueryTestResults("select * from worksFor where yearsWorked = 9", columns("person_OUT_ID", "company_IN_ID", "yearsWorked", "worksFor_ID"),
                rows(r(26L, 2L, 9, 62L)));
    }

    @Test
    public void testSelectNull() throws SQLException {
        runQueryTestResults("SELECT name, age FROM person", columns("name", "age"),
                rows(r("Tom", 35), r("Patty", 29), r("Phil", 31), r("Susan", 45),
                        r("Juanita", null), r("Pavel", 30)));
    }

    // TODO: ORDER BY support with null
    @Test
    @Disabled
    public void testOrderWithNull() throws SQLException {
        // ORDER with integer column.
        runQueryTestResults("SELECT name, age FROM person ORDER BY age", columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Tom", 35),
                        r("Susan", 45), r("Juanita", null)));
    }

    @Test
    public void testWhereNull() throws SQLException {
        // WHERE with string literal.
        runQueryTestResults("SELECT name, age FROM person WHERE age <> 30", columns("name", "age"),
                rows(r("Tom", 35), r("Patty", 29), r("Phil", 31), r("Susan", 45),
                        r("Juanita", null)));
    }

    // TODO: Support aggregates for null values
    @Disabled
    @Test
    public void testHavingNull() throws SQLException {
        // HAVING with aggregate literal.
        runQueryTestResults("SELECT wentToSpace, SUM(age) FROM person GROUP BY wentToSpace HAVING SUM(age) > 1000",
                columns("wentToSpace", "SUM(age)"),
                rows());
        runQueryTestResults("SELECT wentToSpace, COUNT(age) FROM person GROUP BY wentToSpace HAVING COUNT(age) < 1000",
                columns("wentToSpace", "COUNT(age)"),
                rows(r(false, 3L), r(true, 3L)));
        runQueryTestResults("SELECT wentToSpace, COUNT(age) FROM person GROUP BY wentToSpace HAVING COUNT(age) <> 3",
                columns("wentToSpace", "COUNT(age)"),
                rows());
        runQueryTestResults("SELECT COUNT(age), SUM(age) FROM person GROUP BY wentToSpace HAVING SUM(age) > 100",
                columns("COUNT(age)", "SUM(age)"),
                rows(r(3L, 125L)));
        runQueryTestResults("SELECT COUNT(age), SUM(age) FROM person GROUP BY wentToSpace HAVING SUM(age) < 100",
                columns("COUNT(age)", "SUM(age)"),
                rows(r(3L, 95L)));
        runQueryTestResults("SELECT COUNT(age), SUM(age) FROM person GROUP BY age HAVING SUM(age) <> 1000",
                columns("COUNT(age)", "SUM(age)"),
                rows(r(1L, 35L), r(1L, 29L), r(1L, 31L), r(1L, 45L), r(1L, 50L), r(1L, 30L)));
        runQueryTestResults("SELECT COUNT(age), SUM(age) FROM person GROUP BY age HAVING SUM(age) = 35",
                columns("COUNT(age)", "SUM(age)"),
                rows(r(1L, 35L)));

        // Having with AND.
        runQueryTestResults(
                "SELECT COUNT(age), SUM(age) FROM person GROUP BY wentToSpace HAVING SUM(age) <> 1000 AND COUNT(age) = 3",
                columns("COUNT(age)", "SUM(age)"),
                rows(r(3L, 95L), r(3L, 125L)));
        runQueryTestResults(
                "SELECT COUNT(age), SUM(age) FROM person GROUP BY wentToSpace HAVING SUM(age) = 125 AND COUNT(age) = 3",
                columns("COUNT(age)", "SUM(age)"),
                rows(r(3L, 125L)));
        runQueryTestResults(
                "SELECT COUNT(age), SUM(age) FROM person GROUP BY wentToSpace HAVING SUM(age) <> 125 AND COUNT(age) = 3",
                columns("COUNT(age)", "SUM(age)"),
                rows(r(3L, 95L)));
        runQueryTestResults(
                "SELECT COUNT(age), SUM(age) FROM person GROUP BY wentToSpace HAVING SUM(age) <> 125 AND COUNT(age) <> 3",
                columns("COUNT(age)", "SUM(age)"),
                rows());

        // Having with OR.
        runQueryTestResults(
                "SELECT COUNT(age), SUM(age) FROM person GROUP BY wentToSpace HAVING SUM(age) = 1000 OR COUNT(age) = 3",
                columns("COUNT(age)", "SUM(age)"),
                rows(r(3L, 95L), r(3L, 125L)));
        runQueryTestResults(
                "SELECT COUNT(age), SUM(age) FROM person GROUP BY wentToSpace HAVING SUM(age) = 1000 OR COUNT(age) <> 3",
                columns("COUNT(age)", "SUM(age)"),
                rows());
        runQueryTestResults(
                "SELECT COUNT(age), SUM(age) FROM person GROUP BY wentToSpace HAVING SUM(age) = 125 OR COUNT(age) <> 3",
                columns("COUNT(age)", "SUM(age)"),
                rows(r(3L, 125L)));
        runQueryTestResults(
                "SELECT COUNT(age), SUM(age) FROM person GROUP BY wentToSpace HAVING SUM(age) <> 125 OR COUNT(age) <> 3",
                columns("COUNT(age)", "SUM(age)"),
                rows(r(3L, 95L)));
    }

    @Test
    void testLimitNull() throws SQLException {
        // LIMIT 1 tests.
        // Single result query.
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Patty' LIMIT 1",
                columns("name", "age"),
                rows(r("Patty", 29)));
        // Multi result query.
        runQueryTestResults("SELECT name, age FROM person LIMIT 1",
                columns("name", "age"),
                rows(r("Tom", 35)));
    }
}
