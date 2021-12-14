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

public class GremlinSqlJoinTest extends GremlinSqlBaseTest {

    GremlinSqlJoinTest() throws SQLException {
    }

    @Override
    protected DataSet getDataSet() {
        return DataSet.SPACE;
    }

    @Test
    void testJoinSameVertex() throws SQLException {
        runJoinQueryTestResults("SELECT person.name AS name1, person1.name AS name2 FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID)",
                columns("name1", "name2"),
                rows(r("Tom", "Patty"), r("Patty", "Juanita"), r("Phil", "Susan"), r("Susan", "Pavel")));

        runJoinQueryTestResults("SELECT person.name AS name1, person1.name AS name2 FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "GROUP BY person.name, person1.name",
                columns("name1", "name2"),
                rows(r("Tom", "Patty"), r("Patty", "Juanita"), r("Phil", "Susan"), r("Susan", "Pavel")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "GROUP BY person.name",
                columns("name"),
                rows(r("Tom"), r("Patty"), r("Phil"), r("Susan")));

        runJoinQueryTestResults("SELECT person1.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "GROUP BY person1.name",
                columns("name"),
                rows(r("Patty"), r("Juanita"), r("Susan"), r("Pavel")));
    }

    @Test
    void testJoinDiffVertex() throws SQLException {
        runJoinQueryTestResults("SELECT person.name, spaceship.model FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID)",
                columns("name", "model"),
                rows(r("Tom", "delta 1"), r("Patty", "delta 1"), r("Phil", "delta 1"),
                        r("Susan", "delta 2"), r("Juanita", "delta 3"), r("Pavel", "delta 3")));

        runJoinQueryTestResults("SELECT person.name, spaceship.model FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY person.name, spaceship.model",
                columns("name", "model"),
                rows(r("Tom", "delta 1"), r("Patty", "delta 1"), r("Phil", "delta 1"),
                        r("Susan", "delta 2"), r("Juanita", "delta 3"), r("Pavel", "delta 3")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY person.name",
                columns("name"),
                rows(r("Tom"), r("Patty"), r("Phil"), r("Susan"), r("Juanita"), r("Pavel")));

        runJoinQueryTestResults("SELECT spaceship.model FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model",
                columns("model"),
                rows(r("delta 1"), r("delta 2"), r("delta 3")));
    }
}
