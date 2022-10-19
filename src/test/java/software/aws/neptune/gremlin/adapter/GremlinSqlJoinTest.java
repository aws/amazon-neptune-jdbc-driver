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

package software.aws.neptune.gremlin.adapter;

import org.junit.jupiter.api.Test;
import software.aws.neptune.gremlin.adapter.util.SqlGremlinError;

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

        runJoinQueryTestResults("SELECT p.name FROM gremlin.person p " +
                        "INNER JOIN gremlin.person p1 ON (p.friendsWith_OUT_ID = p1.friendsWith_IN_ID) " +
                        "GROUP BY p.name",
                columns("name"),
                rows(r("Tom"), r("Patty"), r("Phil"), r("Susan")));

        runJoinQueryTestResults("SELECT p.name, COUNT(p1.name) FROM gremlin.person p " +
                        "INNER JOIN gremlin.person p1 ON (p.friendsWith_OUT_ID = p1.friendsWith_IN_ID) " +
                        "GROUP BY p.name",
                columns("name", "COUNT(name)"),
                rows(r("Tom", 1L), r("Patty", 1L), r("Phil", 1L), r("Susan", 1L)));
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

    @Test
    void testJoinDiffEdge() {
        // Same vertex label, different edge.
        runQueryTestThrows("SELECT person.name AS name1, person1.name AS name2 FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.worksFor_OUT_ID = person1.friendsWith_IN_ID)",
                SqlGremlinError.CANNOT_JOIN_DIFFERENT_EDGES, "worksFor", "friendsWith");
        runQueryTestThrows("SELECT person.name AS name1, person1.name AS name2 FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_IN_ID = person1.worksFor_OUT_ID)",
                SqlGremlinError.CANNOT_JOIN_DIFFERENT_EDGES, "friendsWith", "worksFor");

        // Different vertex label, different edge.
        runQueryTestThrows("SELECT person.name, spaceship.model FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.worksFor_OUT_ID = spaceship.pilots_IN_ID)",
                SqlGremlinError.CANNOT_JOIN_DIFFERENT_EDGES, "worksFor", "pilots");

        // Different vertex label, different edge.
        runQueryTestThrows("SELECT person.name, spaceship.model FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (spaceship.pilots_IN_ID = person.worksFor_OUT_ID)",
                SqlGremlinError.CANNOT_JOIN_DIFFERENT_EDGES, "worksFor", "pilots");
    }

    @Test
    void testJoinWhere() throws SQLException {
        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.name = 'Tom'" +
                        "GROUP BY person.name",
                columns("name"),
                rows(r("Tom")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.name = 'Tom'",
                columns("name"),
                rows(r("Tom")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.name <> 'Tom'",
                columns("name"),
                rows(r("Patty"), r("Phil"), r("Susan")));

        runJoinQueryTestResults("SELECT person.name, person.wentToSpace FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.wentToSpace",
                columns("name", "wentToSpace"),
                rows(r("Susan", true)));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE NOT person.wentToSpace",
                columns("name"),
                rows(r("Tom"), r("Patty"), r("Phil")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.age = 35",
                columns("name"),
                rows(r("Tom")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.age >= 35",
                columns("name"),
                rows(r("Tom"), r("Susan")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.age <= 35",
                columns("name"),
                rows(r("Tom"), r("Patty"), r("Phil")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.age < 35",
                columns("name"),
                rows(r("Patty"), r("Phil")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.age > 35",
                columns("name"),
                rows(r("Susan")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.age <> 35",
                columns("name"),
                rows(r("Patty"), r("Phil"), r("Susan")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.age >= 35 AND person.name = 'Tom'",
                columns("name"),
                rows(r("Tom")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.age <> 35 OR person.name = 'Tom'",
                columns("name"),
                rows(r("Tom"), r("Patty"), r("Phil"), r("Susan")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE person.age <= 35 AND person.name <> 'Tom' AND NOT person.wentToSpace",
                columns("name"),
                rows(r("Patty"), r("Phil")));

        runJoinQueryTestResults("SELECT person.name FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON (person.friendsWith_OUT_ID = person1.friendsWith_IN_ID) " +
                        "WHERE NOT person.name = 'Tom'",
                columns("name"),
                rows(r("Patty"), r("Phil"), r("Susan")));
    }

    @Test
    void testJoinHaving() throws SQLException {
        runJoinQueryTestResults( "SELECT person.name, COUNT(person1.age) FROM gremlin.person person " +
                        "INNER JOIN gremlin.person person1 ON person.friendsWith_OUT_ID = person1.friendsWith_IN_ID " +
                        "GROUP BY person.name HAVING COUNT(person1.age) > 0",
                columns("name", "COUNT(age)"),
                rows(r("Tom", 1L), r("Patty", 1L), r("Phil", 1L), r("Susan", 1L)));

        runJoinQueryTestResults("SELECT spaceship.model, COUNT(person.name) FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING COUNT(person.name) >= 1",
                columns("model", "COUNT(name)"),
                rows(r("delta 1", 3L), r("delta 2", 1L), r("delta 3", 2L)));

        runJoinQueryTestResults("SELECT spaceship.model, COUNT(person.name) FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING COUNT(person.name) <> 2",
                columns("model", "COUNT(name)"),
                rows(r("delta 1", 3L), r("delta 2", 1L)));

        runJoinQueryTestResults("SELECT spaceship.model, COUNT(person.name) FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING COUNT(person.name) > 1",
                columns("model", "COUNT(name)"),
                rows(r("delta 1", 3L), r("delta 3", 2L)));

        runJoinQueryTestResults("SELECT spaceship.model, COUNT(person.name) FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING COUNT(person.name) <= 1 OR COUNT(person.name) >= 3",
                columns("model", "COUNT(name)"),
                rows(r("delta 1", 3L), r("delta 2", 1L)));

        runJoinQueryTestResults("SELECT spaceship.model, COUNT(person.name) FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING COUNT(person.name) <= 1",
                columns("model", "COUNT(name)"),
                rows(r("delta 2", 1L)));

        runJoinQueryTestResults("SELECT spaceship.model, SUM(person.age) FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING SUM(person.age) > 50",
                columns("model", "SUM(age)"),
                rows(r("delta 1", 95L), r("delta 3", 80L)));

        runJoinQueryTestResults("SELECT spaceship.model, SUM(person.age) FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING SUM(person.age) <> 80",
                columns("model", "SUM(age)"),
                rows(r("delta 1", 95L), r("delta 2", 45L)));

        runJoinQueryTestResults("SELECT spaceship.model, SUM(person.age) FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING SUM(person.age) <> 95",
                columns("model", "SUM(age)"),
                rows(r("delta 2", 45L), r("delta 3", 80L)));

        runJoinQueryTestResults("SELECT spaceship.model, MAX(person.age) FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING MAX(person.age) > 40",
                columns("model", "MAX(age)"),
                rows(r("delta 2", 45), r("delta 3", 50)));

        runJoinQueryTestResults("SELECT spaceship.model, MIN(person.age) FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING MIN(person.age) < 40",
                columns("model", "MIN(age)"),
                rows(r("delta 1", 29), r("delta 3", 30)));

        runJoinQueryTestResults("SELECT spaceship.model, AVG(person.age) FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING AVG(person.age) > 35",
                columns("model", "AVG(age)"),
                rows(r("delta 2", 45.0), r("delta 3", 40.0)));

        runJoinQueryTestResults("SELECT spaceship.model FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING MIN(person.age) < 40 AND AVG(person.age) > 35",
                columns("model"),
                rows(r("delta 3")));

        runJoinQueryTestResults("SELECT spaceship.model FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING MIN(person.age) < 40 OR AVG(person.age) > 35",
                columns("model"),
                rows(r("delta 1"), r("delta 2"), r("delta 3")));

        runJoinQueryTestResults("SELECT spaceship.model FROM gremlin.person person " +
                        "INNER JOIN gremlin.spaceship spaceship ON (person.pilots_OUT_ID = spaceship.pilots_IN_ID) " +
                        "GROUP BY spaceship.model HAVING spaceship.model = 'delta 2'",
                columns("model"),
                rows(r("delta 2")));
    }
}
