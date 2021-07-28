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

package software.aws.neptune.opencypher;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.aws.neptune.NeptunePreparedStatementTestHelper;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.opencypher.mock.MockOpenCypherDatabase;
import java.sql.SQLException;
import java.util.Properties;

public class OpenCypherPreparedStatementTest extends OpenCypherStatementTestBase {
    private static final String HOSTNAME = "localhost";
    private static final Properties PROPERTIES = new Properties();
    private static MockOpenCypherDatabase database;
    private NeptunePreparedStatementTestHelper neptunePreparedStatementTestHelper;

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() throws SQLException {
        database = MockOpenCypherDatabase.builder(HOSTNAME, OpenCypherPreparedStatementTest.class.getName()).build();
        PROPERTIES.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reverse default to None
        PROPERTIES.putIfAbsent(OpenCypherConnectionProperties.ENDPOINT_KEY,
                String.format("bolt://%s:%d", HOSTNAME, database.getPort()));
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        database.shutdown();
    }

    @BeforeEach
    void initialize() throws SQLException {
        final java.sql.Connection connection = new OpenCypherConnection(new OpenCypherConnectionProperties(PROPERTIES));
        neptunePreparedStatementTestHelper = new NeptunePreparedStatementTestHelper(connection.prepareStatement(""),
                connection.prepareStatement(LONG_QUERY), connection.prepareStatement(QUICK_QUERY));
    }

    @Test
    void testCancelQueryWithoutExecute() {
        neptunePreparedStatementTestHelper.testCancelQueryWithoutExecute();
    }

    @Test
    void testCancelQueryWhileExecuteInProgress() {
        neptunePreparedStatementTestHelper.testCancelQueryWhileExecuteInProgress();
    }

    @Test
    void testCancelQueryTwice() {
        neptunePreparedStatementTestHelper.testCancelQueryTwice();
    }

    @Test
    void testCancelQueryAfterExecuteComplete() {
        neptunePreparedStatementTestHelper.testCancelQueryAfterExecuteComplete();
    }

    @Test
    void testMisc() {
        neptunePreparedStatementTestHelper.testMisc();
    }

    @Test
    void testSet() {
        neptunePreparedStatementTestHelper.testSet();
    }
}
