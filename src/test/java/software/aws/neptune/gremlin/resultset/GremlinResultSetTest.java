/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.gremlin.resultset;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.aws.neptune.gremlin.GremlinConnection;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.neptune.gremlin.mock.MockGremlinDatabase;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static software.aws.neptune.gremlin.GremlinHelper.createVertex;
import static software.aws.neptune.gremlin.GremlinHelper.dropVertex;
import static software.aws.neptune.gremlin.GremlinHelper.getProperties;
import static software.aws.neptune.gremlin.GremlinHelper.getVertex;

class GremlinResultSetTest {
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 8181; // Mock server uses 8181.
    private static final String VERTEX = "planet";

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final Map<String, Object> VERTEX_PROPERTIES_MAP = new HashMap();
    private static java.sql.Connection connection;
    private static java.sql.ResultSet resultSet;

    static {
        VERTEX_PROPERTIES_MAP.put("name", "Earth");
        VERTEX_PROPERTIES_MAP.put("continents", 7);
        VERTEX_PROPERTIES_MAP.put("diameter", 12742);
        VERTEX_PROPERTIES_MAP.put("age", 4500000000L);
        VERTEX_PROPERTIES_MAP.put("tilt", 23.4392811);
        VERTEX_PROPERTIES_MAP.put("density", 5.514F);
        VERTEX_PROPERTIES_MAP.put("supportsLife", true);
    }

    @BeforeAll
    static void beforeAll() throws SQLException, IOException, InterruptedException {
        MockGremlinDatabase.startGraph();
        connection = new GremlinConnection(new GremlinConnectionProperties(getProperties(HOSTNAME, PORT)));
        createVertex(connection, VERTEX, VERTEX_PROPERTIES_MAP);
        resultSet = getVertex(connection, VERTEX);
        Assertions.assertNotNull(resultSet);
        Assertions.assertDoesNotThrow(() -> resultSet.next());
    }

    @AfterAll
    static void shutdown() throws SQLException, IOException, InterruptedException {
        dropVertex(connection, VERTEX);
        connection.close();
        MockGremlinDatabase.stopGraph();
    }

    @Test
    void testBooleanType() throws SQLException {
        final int col = resultSet.findColumn("supportsLife");
        Assertions.assertTrue(resultSet.getBoolean(col));
        Assertions.assertEquals(((Boolean) true).toString(), resultSet.getString(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(col));
    }

    @Test
    void testShortType() throws SQLException {
        final int col = resultSet.findColumn("continents");
        final int expected = (int) VERTEX_PROPERTIES_MAP.get("continents");
        Assertions.assertEquals(expected, 7);
        Assertions.assertEquals((byte) expected, resultSet.getByte(col));
        Assertions.assertEquals((short) expected, resultSet.getShort(col));
        Assertions.assertEquals(expected, resultSet.getInt(col));
        Assertions.assertEquals(expected, resultSet.getLong(col));
        Assertions.assertEquals(expected, resultSet.getDouble(col));
        Assertions.assertEquals(expected, resultSet.getFloat(col));
        Assertions.assertEquals(((Integer) expected).toString(), resultSet.getString(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(col));
    }

    @Test
    void testIntegerType() throws SQLException {
        final int col = resultSet.findColumn("diameter");
        final int expected = (int) VERTEX_PROPERTIES_MAP.get("diameter");
        Assertions.assertEquals(expected, 12742);
        Assertions.assertEquals(expected, resultSet.getInt(col));
        Assertions.assertEquals(expected, resultSet.getLong(col));
        Assertions.assertEquals(expected, resultSet.getDouble(col));
        Assertions.assertEquals(expected, resultSet.getFloat(col));
        Assertions.assertEquals(((Integer) expected).toString(), resultSet.getString(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(col));
    }

    @Test
    void testLongType() throws SQLException {
        final int col = resultSet.findColumn("age");
        final long expected = (long) VERTEX_PROPERTIES_MAP.get("age");
        Assertions.assertEquals(expected, 4500000000L);
        Assertions.assertEquals(expected, resultSet.getLong(col));
        Assertions.assertEquals(expected, resultSet.getDouble(col));
        Assertions.assertEquals(expected, resultSet.getFloat(col));
        Assertions.assertEquals(((Long) expected).toString(), resultSet.getString(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(col));
    }

    @Test
    void testDoubleType() throws SQLException {
        // TODO AN-813: Fix insert type of data.
        final int col = resultSet.findColumn("tilt");
        final String expected = VERTEX_PROPERTIES_MAP.get("tilt").toString();
        Assertions.assertEquals(expected, resultSet.getString(col));
    }

    @Test
    void testFloatingPointType() throws SQLException {
        // TODO AN-813: Fix insert type of data.
        final int col = resultSet.findColumn("density");
        final String expected = VERTEX_PROPERTIES_MAP.get("density").toString();
        Assertions.assertEquals(expected, resultSet.getString(col));
    }

    @Test
    void testStringType() throws SQLException {
        final int col = resultSet.findColumn("name");
        final String expected = (String) VERTEX_PROPERTIES_MAP.get("name");
        Assertions.assertEquals(expected, "Earth");
        Assertions.assertEquals(expected, resultSet.getString(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getBoolean(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getByte(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getShort(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getInt(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getLong(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getDouble(col));
        Assertions.assertThrows(SQLException.class, () -> resultSet.getFloat(col));
    }

    @Test
    void testScalarResult() throws SQLException {
        final GremlinResultSet scalarResultSet = (GremlinResultSet) connection.createStatement()
                .executeQuery("g.V().count()");
        Assertions.assertNotNull(scalarResultSet);
        Assertions.assertDoesNotThrow(scalarResultSet::next);

        final int col = scalarResultSet.findColumn("_col0");
        Assertions.assertEquals(1, scalarResultSet.getLong(col));
    }

    @Test
    void testMultipleScalarResults() throws SQLException {
        final GremlinResultSet scalarResultSet = (GremlinResultSet) connection.createStatement()
                .executeQuery(String.format("g.V().hasLabel('%s').properties().key()", VERTEX));
        Assertions.assertNotNull(scalarResultSet);
        int unnamedColumnsFound = 0;

        while (scalarResultSet.next()) {
            final int col = scalarResultSet.findColumn("_col" + unnamedColumnsFound);
            Assertions.assertTrue(VERTEX_PROPERTIES_MAP.containsKey(scalarResultSet.getString(col)));
            unnamedColumnsFound++;
        }

        Assertions.assertEquals(VERTEX_PROPERTIES_MAP.keySet().size(), unnamedColumnsFound);
    }
}

