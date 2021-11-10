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
    public void testWhereNumeric() throws SQLException {
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
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' AND age = 35 ORDER BY age", columns("name", "age"),
                rows(r("Tom", 35)));
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' AND age = 35 AND NOT wentToSpace ORDER BY age", columns("name", "age"),
                rows(r("Tom", 35)));
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' AND age = 35 AND wentToSpace ORDER BY age", columns("name", "age"),
                rows());
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Pavel' AND age = 30 AND wentToSpace ORDER BY age", columns("name", "age"),
                rows(r("Pavel", 30)));

        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' OR name = 'Juanita' ORDER BY age", columns("name", "age"),
                rows(r("Tom", 35), r("Juanita", 50)));
        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' OR name = 'Juanita' OR age = 31 ORDER BY age", columns("name", "age"),
                rows(r("Phil", 31), r("Tom", 35), r("Juanita", 50)));

        runQueryTestResults("SELECT name, age FROM person WHERE name = 'Tom' OR name = 'Juanita' ORDER BY age", columns("name", "age"),
                rows(r("Tom", 35), r("Juanita", 50)));
    }
}
