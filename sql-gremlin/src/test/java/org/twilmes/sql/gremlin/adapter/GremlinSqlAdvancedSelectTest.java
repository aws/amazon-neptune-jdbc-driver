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
import org.twilmes.sql.gremlin.adapter.util.SqlGremlinError;

import java.math.BigDecimal;
import java.sql.SQLException;

/**
 * Created by twilmes on 12/7/15.
 */
public class GremlinSqlAdvancedSelectTest extends GremlinSqlBaseTest {

    GremlinSqlAdvancedSelectTest() throws SQLException {
    }

    @Override
    protected DataSet getDataSet() {
        return DataSet.SPACE;
    }

    @Test
    public void testProject() throws SQLException {
        runQueryTestResults("select name from person", columns("name"),
                rows(r("Tom"), r("Patty"), r("Phil"), r("Susan"), r("Juanita"), r("Pavel")));
    }

    @Test
    public void testEdges() throws SQLException {
        runQueryTestResults("select * from worksFor where yearsWorked = 9", columns("person_OUT_ID", "company_IN_ID", "yearsWorked", "worksFor_ID"),
                rows(r(26L, 2L, 9, 64L)));
    }

    @Test
    public void testOrder() throws SQLException {
        // ORDER with integer column.
        runQueryTestResults("SELECT name, age FROM person ORDER BY age", columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Tom", 35), r("Susan", 45), r("Juanita", 50)));
        runQueryTestResults("SELECT name, age FROM person ORDER BY age DESC", columns("name", "age"),
                rows(r("Juanita", 50), r("Susan", 45), r("Tom", 35), r("Phil", 31), r("Pavel", 30), r("Patty", 29)));

        runQueryTestResults("SELECT name, age AS a FROM person ORDER BY a", columns("name", "a"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Tom", 35), r("Susan", 45), r("Juanita", 50)));
        runQueryTestResults("SELECT name, age AS a FROM person ORDER BY a DESC", columns("name", "a"),
                rows(r("Juanita", 50), r("Susan", 45), r("Tom", 35), r("Phil", 31), r("Pavel", 30), r("Patty", 29)));

        // ORDER with string column.
        runQueryTestResults("SELECT name, age FROM person ORDER BY name", columns("name", "age"),
                rows(r("Juanita", 50), r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Susan", 45), r("Tom", 35)));
        runQueryTestResults("SELECT name, age FROM person ORDER BY name DESC", columns("name", "age"),
                rows(r("Tom", 35), r("Susan", 45), r("Phil", 31), r("Pavel", 30), r("Patty", 29), r("Juanita", 50)));

        runQueryTestResults("SELECT name AS n, age AS a FROM person ORDER BY n", columns("n", "a"),
                rows(r("Juanita", 50), r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Susan", 45), r("Tom", 35)));
        runQueryTestResults("SELECT name AS n, age AS a FROM person ORDER BY n DESC", columns("n", "a"),
                rows(r("Tom", 35), r("Susan", 45), r("Phil", 31), r("Pavel", 30), r("Patty", 29), r("Juanita", 50)));
    }

    @Test
    public void testWhere() throws SQLException {
        // WHERE with string literal.
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' ORDER BY age", columns("name", "age"),
                rows(r("Tom", 35)));
        runQueryTestResults("SELECT name, age FROM person WHERE name <> 'Tom' ORDER BY age", columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Susan", 45), r("Juanita", 50)));

        // WHERE with boolean literal.
        runQueryTestResults("SELECT name, age FROM person WHERE wentToSpace ORDER BY age", columns("name", "age"),
                rows(r("Pavel", 30), r("Susan", 45), r("Juanita", 50)));
        runQueryTestResults("SELECT name, age FROM person WHERE NOT wentToSpace ORDER BY age", columns("name", "age"),
                rows(r("Patty", 29), r("Phil", 31), r("Tom", 35)));
        runQueryTestResults("SELECT name, age FROM person WHERE wentToSpace = 1 ORDER BY age", columns("name", "age"),
                rows(r("Pavel", 30), r("Susan", 45), r("Juanita", 50)));
        runQueryTestResults("SELECT name, age FROM person WHERE wentToSpace = 0 ORDER BY age", columns("name", "age"),
                rows(r("Patty", 29), r("Phil", 31), r("Tom", 35)));

        // WHERE with numeric literal.
        runQueryTestResults("SELECT name, age FROM person WHERE age = 35 ORDER BY age", columns("name", "age"),
                rows(r("Tom", 35)));
        runQueryTestResults("SELECT name, age FROM person WHERE age >= 35 ORDER BY age", columns("name", "age"),
                rows(r("Tom", 35), r("Susan", 45), r("Juanita", 50)));
        runQueryTestResults("SELECT name, age FROM person WHERE age <= 35 ORDER BY age", columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Tom", 35)));
        runQueryTestResults("SELECT name, age FROM person WHERE age < 35 ORDER BY age", columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31)));
        runQueryTestResults("SELECT name, age FROM person WHERE age > 35 ORDER BY age", columns("name", "age"),
                rows(r("Susan", 45), r("Juanita", 50)));

        // WHERE with numeric literal and descending order (just for fun).
        runQueryTestResults("SELECT name, age FROM person WHERE age >= 35 ORDER BY age DESC", columns("name", "age"),
                rows(r("Juanita", 50), r("Susan", 45), r("Tom", 35)));
        runQueryTestResults("SELECT name, age FROM person WHERE age <= 35 ORDER BY age DESC", columns("name", "age"),
                rows(r("Tom", 35), r("Phil", 31), r("Pavel", 30), r("Patty", 29)));
        runQueryTestResults("SELECT name, age FROM person WHERE age < 35 ORDER BY age DESC", columns("name", "age"),
                rows(r("Phil", 31), r("Pavel", 30), r("Patty", 29)));
        runQueryTestResults("SELECT name, age FROM person WHERE age > 35 ORDER BY age DESC", columns("name", "age"),
                rows(r("Juanita", 50), r("Susan", 45)));

        // WHERE with AND.
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' AND age = 35 ORDER BY age",
                columns("name", "age"),
                rows(r("Tom", 35)));
        runQueryTestResults(
                "SELECT name, age FROM person WHERE name = 'Tom' AND age = 35 AND NOT wentToSpace ORDER BY age",
                columns("name", "age"),
                rows(r("Tom", 35)));
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' AND age = 35 AND wentToSpace ORDER BY age",
                columns("name", "age"),
                rows());
        runQueryTestResults(
                "SELECT name, age FROM person WHERE name = 'Pavel' AND age = 30 AND wentToSpace ORDER BY age",
                columns("name", "age"),
                rows(r("Pavel", 30)));

        // WHERE with OR.
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' OR name = 'Juanita' ORDER BY age",
                columns("name", "age"),
                rows(r("Tom", 35), r("Juanita", 50)));
        runQueryTestResults(
                "SELECT name, age FROM person WHERE name = 'Tom' OR name = 'Juanita' OR age = 31 ORDER BY age",
                columns("name", "age"),
                rows(r("Phil", 31), r("Tom", 35), r("Juanita", 50)));
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' OR name = 'Juanita' ORDER BY age",
                columns("name", "age"),
                rows(r("Tom", 35), r("Juanita", 50)));
    }

    @Test
    public void testHaving() throws SQLException {
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
    void testMisc() throws SQLException {
        runQueryTestResults(
                "SELECT wentToSpace, COUNT(wentToSpace) FROM person GROUP BY wentToSpace HAVING COUNT(wentToSpace) > 1",
                columns("wentToSpace", "COUNT(wentToSpace)"), rows(r(false, 3L), r(true, 3L)));
        runQueryTestResults("SELECT COUNT(name), COUNT(name) FROM person error", columns("COUNT(name)", "COUNT(name)"),
                rows(r(6L, 6L)));
        runQueryTestResults("SELECT SUM(age) FROM \"gremlin\".\"person\" WHERE name = 'test'", columns("SUM(age)"),
                rows());
        runQueryTestResults("SELECT COUNT(name), COUNT(*) FROM person error", columns("COUNT(name)", "COUNT(*)"),
                rows(r(6L, 6L)));
        runQueryTestResults("SELECT name, COUNT(name), count(1) from \"gremlin\".\"person\" group by name",
                columns("name", "COUNT(name)", "COUNT(1)"),
                rows(r("Tom", 1L, 1L), r("Patty", 1L, 1L), r("Phil", 1L, 1L),
                        r("Susan", 1L, 1L), r("Juanita", 1L, 1L), r("Pavel", 1L, 1L)));
        runQueryTestResults("SELECT name, COUNT(name), count(1) AS cnt from \"gremlin\".\"person\" group by name",
                columns("name", "COUNT(name)", "cnt"),
                rows(r("Tom", 1L, 1L), r("Patty", 1L, 1L), r("Phil", 1L, 1L),
                        r("Susan", 1L, 1L), r("Juanita", 1L, 1L), r("Pavel", 1L, 1L)));
        runQueryTestResults(
                "SELECT wentToSpace, COUNT(name), SUM(age), AVG(age), MIN(age), MAX(age) FROM \"gremlin\".\"person\" GROUP BY wentToSpace",
                columns("wentToSpace", "COUNT(name)", "SUM(age)", "AVG(age)", "MIN(age)", "MAX(age)"),
                rows(
                        r(false, 3L, 95L, 31.666666666666668, 29, 35),
                        r(true, 3L, 125L, 41.666666666666664, 30, 50))
        );
        runQueryTestResults("SELECT name FROM \"gremlin\".\"person\" ORDER BY 1",
                columns("name"),
                rows(r("Juanita"), r("Patty"), r("Pavel"), r("Phil"), r("Susan"), r("Tom")));
        runQueryTestResults("SELECT DISTINCT name FROM \"gremlin\".\"person\" ORDER BY name",
                columns("name"),
                rows(r("Juanita"), r("Patty"), r("Pavel"), r("Phil"), r("Susan"), r("Tom")));
        runQueryTestResults("SELECT DISTINCT name FROM \"gremlin\".\"person\" ORDER BY name DESC",
                columns("name"),
                rows(r("Tom"), r("Susan"), r("Phil"), r("Pavel"), r("Patty"), r("Juanita")));

        // NULLS FIRST predicate is not currently supported.
        runQueryTestThrows("SELECT name FROM \"gremlin\".\"person\" ORDER BY name NULLS FIRST",
                SqlGremlinError.NO_ORDER, "NULLS_FIRST");
    }

    @Test
    void testAggregateLiteralHavingNoGroupBy() throws SQLException {
        // Tableau was sending queries like this for the preview in 2021.3
        runQueryTestResults(
                "SELECT SUM(1) AS \"cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok\" FROM gremlin.person AS person HAVING COUNT(1) > 0",
                columns("cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok"),
                rows(r(new BigDecimal(6))));
        runQueryTestResults(
                "SELECT MIN(1) AS \"cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok\" FROM gremlin.person AS person HAVING COUNT(1) > 0",
                columns("cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok"),
                rows(r(new BigDecimal(1))));
        runQueryTestResults(
                "SELECT MAX(1) AS \"cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok\" FROM gremlin.person AS person HAVING COUNT(1) > 0",
                columns("cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok"),
                rows(r(new BigDecimal(1))));
        runQueryTestResults(
                "SELECT AVG(1) AS \"cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok\" FROM gremlin.person AS person HAVING COUNT(1) > 0",
                columns("cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok"),
                rows(r(new BigDecimal(1))));

        runQueryTestResults(
                "SELECT SUM(2) AS \"cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok\" FROM gremlin.person AS person HAVING COUNT(1) > 0",
                columns("cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok"),
                rows(r(new BigDecimal(12))));
        runQueryTestResults(
                "SELECT MIN(2) AS \"cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok\" FROM gremlin.person AS person HAVING COUNT(1) > 0",
                columns("cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok"),
                rows(r(new BigDecimal(2))));
        runQueryTestResults(
                "SELECT MAX(2) AS \"cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok\" FROM gremlin.person AS person HAVING COUNT(1) > 0",
                columns("cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok"),
                rows(r(new BigDecimal(2))));
        runQueryTestResults(
                "SELECT AVG(2) AS \"cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok\" FROM gremlin.person AS person HAVING COUNT(1) > 0",
                columns("cnt:airport_03C2E834E28942D3AA2423AC01F4B33D:ok"),
                rows(r(new BigDecimal(2))));
    }

    @Test
    void testLimit() throws SQLException {
        // LIMIT 1 tests.
        // Single result query.
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' ORDER BY age LIMIT 1",
                columns("name", "age"),
                rows(r("Tom", 35)));
        // Multi result query.
        runQueryTestResults("SELECT name, age FROM person ORDER BY age LIMIT 1",
                columns("name", "age"),
                rows(r("Patty", 29)));

        // LIMIT > 1 tests.
        runQueryTestResults("SELECT name, age FROM person WHERE name <> 'Tom' ORDER BY age LIMIT 2",
                columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30)));
        runQueryTestResults("SELECT name, age FROM person WHERE name <> 'Tom' ORDER BY age LIMIT 3",
                columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31)));
        runQueryTestResults("SELECT name, age FROM person WHERE name <> 'Tom' ORDER BY age LIMIT 4",
                columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Susan", 45)));
        runQueryTestResults("SELECT name, age FROM person WHERE name <> 'Tom' ORDER BY age LIMIT 5",
                columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Susan", 45), r("Juanita", 50)));
        runQueryTestResults("SELECT name, age FROM person WHERE name <> 'Tom' ORDER BY age LIMIT 6",
                columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Susan", 45), r("Juanita", 50)));
        runQueryTestResults("SELECT name, age FROM person WHERE name <> 'Tom' ORDER BY age LIMIT 1000",
                columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Susan", 45), r("Juanita", 50)));
        runQueryTestResults(
                String.format("SELECT name, age FROM person WHERE name <> 'Tom' ORDER BY age LIMIT %d", Long.MAX_VALUE),
                columns("name", "age"),
                rows(r("Patty", 29), r("Pavel", 30), r("Phil", 31), r("Susan", 45), r("Juanita", 50)));
    }
}
