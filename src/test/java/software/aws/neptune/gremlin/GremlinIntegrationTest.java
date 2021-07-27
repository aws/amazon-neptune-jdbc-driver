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

package software.aws.neptune.gremlin;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.jdbc.helpers.HelperFunctions;
import software.aws.jdbc.utilities.ConnectionProperties;
import software.aws.jdbc.utilities.SqlError;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static software.aws.neptune.gremlin.GremlinConnectionProperties.CONTACT_POINT_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.ENABLE_SSL_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.PORT_KEY;
import static software.aws.neptune.gremlin.GremlinHelper.createVertex;
import static software.aws.neptune.gremlin.GremlinHelper.dropAll;
import static software.aws.neptune.gremlin.GremlinHelper.dropVertex;
import static software.aws.neptune.gremlin.GremlinHelper.getAll;
import static software.aws.neptune.gremlin.GremlinHelper.getVertex;

@Disabled
public class GremlinIntegrationTest extends GremlinStatementTestBase {
    private static final int PORT = 8182;
    private static final String ENDPOINT = "iam-auth-test-lyndon.cluster-cdubgfjknn5r.us-east-1.neptune.amazonaws.com";
    private static final String AUTH = "IamSigV4";
    private static final String ENCRYPTION = "TRUE";
    private static final String CONNECTION_STRING =
            String.format("jdbc:neptune:gremlin://%s;enableSsl=%s;authScheme=%s;",
                    ENDPOINT, ENCRYPTION, AUTH);
    private static final String VERTEX_1 = "vertex1";
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final Map<String, Object> VERTEX_1_MAP = new HashMap();
    private static final String VERTEX_2 = "vertex2";
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final Map<String, Object> VERTEX_2_MAP = new HashMap();
    private static final Map<String, Object> ALL_MAP = new HashMap<>();
    private static java.sql.Connection connection;

    static {
        VERTEX_1_MAP.put("A", 1);
        VERTEX_1_MAP.put("B", 2);
        VERTEX_1_MAP.put("C", 3);
        VERTEX_1_MAP.put("D", 4);
    }

    static {
        VERTEX_2_MAP.put("D", 1);
        VERTEX_2_MAP.put("E", 2);
    }

    static {
        ALL_MAP.putAll(VERTEX_1_MAP);
        ALL_MAP.putAll(VERTEX_2_MAP);
    }

    @BeforeAll
    static void initialize() throws SQLException, IOException, InterruptedException {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AUTH);
        properties.put(CONTACT_POINT_KEY, ENDPOINT);
        properties.put(PORT_KEY, PORT);
        properties.put(ENABLE_SSL_KEY, ENCRYPTION);
        connection = new GremlinConnection(new GremlinConnectionProperties(properties));
        dropAll(connection);
    }

    @AfterAll
    static void shutdown() throws SQLException, IOException {
        dropAll(connection);
        connection.close();
    }

    private static Set<String> getActualColumns(final java.sql.ResultSet resultSet) throws SQLException {
        Assertions.assertNotNull(resultSet);
        final Set<String> actualColumns = new HashSet<>();
        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
            actualColumns.add(resultSet.getMetaData().getColumnName(i));
        }
        return actualColumns;
    }

    private static void validateResultSetColumns(final java.sql.ResultSet resultSet,
                                                 final Set<String> expectedColumns) throws SQLException {
        Assertions.assertEquals((long) expectedColumns.size(), resultSet.getMetaData().getColumnCount());
        Assertions.assertEquals(expectedColumns, getActualColumns(resultSet));
    }

    private static void validateResultSetRows(final java.sql.ResultSet resultSet,
                                              final Map<String, Object> properties) throws SQLException {
        for (final String col : getActualColumns(resultSet)) {
            Assertions.assertEquals(resultSet.getInt(col), properties.get(col));
        }
    }

    @Test
    void testVertexStructure() throws SQLException {
        createVertex(connection, VERTEX_1, VERTEX_1_MAP);
        createVertex(connection, VERTEX_2, VERTEX_2_MAP);

        final java.sql.ResultSet resultSet1 = getVertex(connection, VERTEX_1);
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        validateResultSetColumns(resultSet1, VERTEX_1_MAP.keySet());
        validateResultSetRows(resultSet1, VERTEX_1_MAP);

        final java.sql.ResultSet resultSet2 = getVertex(connection, VERTEX_2);
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        validateResultSetColumns(resultSet2, VERTEX_2_MAP.keySet());
        validateResultSetRows(resultSet2, VERTEX_2_MAP);

        final java.sql.ResultSet resultSetAll = getAll(connection);
        Assertions.assertNotNull(resultSetAll);
        Assertions.assertTrue(resultSetAll.next());
        validateResultSetColumns(resultSetAll, ALL_MAP.keySet());

        dropVertex(connection, VERTEX_1);
        dropVertex(connection, VERTEX_2);
    }

    @Test
    void driverManagerTest() throws SQLException {
        final java.sql.Connection conn = DriverManager.getConnection(CONNECTION_STRING);
        Assertions.assertTrue(conn.isValid(1));
    }

    @Test
    void cancelQueryTest() throws SQLException {
        final java.sql.Statement statement = connection.createStatement();
        launchCancelThread(150, statement);
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED, () -> statement.execute(getLongQuery()));
        waitCancelToComplete();
    }
}
