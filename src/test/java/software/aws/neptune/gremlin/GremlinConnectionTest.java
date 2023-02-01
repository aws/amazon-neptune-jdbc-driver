/*
 * Copyright <2023> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.aws.neptune.gremlin.mock.MockGremlinDatabase;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import static software.aws.neptune.gremlin.GremlinHelper.getProperties;

public class GremlinConnectionTest {
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 8181; // Mock server uses 8181.
    private static final String QUERY = "1+1";
    private static final Properties PROPERTIES = getProperties(HOSTNAME, PORT);
    private java.sql.Connection connection;

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() throws IOException, InterruptedException {
        MockGremlinDatabase.startGraph();
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() throws IOException, InterruptedException {
        MockGremlinDatabase.stopGraph();
    }

    @BeforeEach
    void initialize() throws SQLException {
        connection = new GremlinConnection(new GremlinConnectionProperties(PROPERTIES));
    }

    @AfterEach
    void shutdown() throws SQLException {
        connection.close();
    }

    @Test
    void testGremlinDatabase() throws SQLException {
        connection.createStatement().executeQuery(QUERY);
    }

    @Test
    void testIsValid() throws SQLException {
        Assertions.assertTrue(connection.isValid(1));

        final Throwable negativeTimeout = Assertions.assertThrows(SQLException.class,
                () -> connection.isValid(-1));
        Assertions.assertEquals("Timeout value must be greater than or equal to 0",
                negativeTimeout.getMessage());

        final java.sql.Connection invalidConnection = new GremlinConnection(
                new GremlinConnectionProperties(getProperties(HOSTNAME, 1234)));
        Assertions.assertFalse(invalidConnection.isValid(1));
    }
}
