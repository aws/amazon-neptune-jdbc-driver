/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License testIs located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file testIs distributed
 * on an "AS testIs" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.neptune.opencypher;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.neptune.NeptuneStatementTestHelper;
import software.amazon.neptune.opencypher.mock.MockOpenCypherDatabase;
import java.sql.SQLException;
import java.util.Properties;

public class OpenCypherStatementTest extends OpenCypherStatementTestBase {
    protected static final String HOSTNAME = "localhost";
    protected static final Properties PROPERTIES = new Properties();
    private static MockOpenCypherDatabase database;
    private static NeptuneStatementTestHelper neptuneStatementTestHelper;

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() throws SQLException {
        database = MockOpenCypherDatabase.builder(HOSTNAME, OpenCypherStatementTest.class.getName()).build();
        PROPERTIES.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reverse default to None
        PROPERTIES.putIfAbsent(OpenCypherConnectionProperties.ENDPOINT_KEY,
                String.format("bolt://%s:%d", HOSTNAME, database.getPort()));
        final java.sql.Connection connection = new OpenCypherConnection(new OpenCypherConnectionProperties(PROPERTIES));
        neptuneStatementTestHelper =
                new NeptuneStatementTestHelper(connection.createStatement(), LONG_QUERY, QUICK_QUERY);
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        database.shutdown();
    }

    @Test
    void testCancelQueryWithoutExecute() {
        neptuneStatementTestHelper.testCancelQueryWithoutExecute();
    }

    @Test
    void testCancelQueryWhileExecuteInProgress() {
        neptuneStatementTestHelper.testCancelQueryWhileExecuteInProgress();
    }

    @Test
    void testCancelQueryTwice() {
        neptuneStatementTestHelper.testCancelQueryTwice();
    }

    @Test
    void testCancelQueryAfterExecuteComplete() {
        neptuneStatementTestHelper.testCancelQueryAfterExecuteComplete();
    }
}
