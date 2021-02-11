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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.Statement;
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.neptune.opencypher.mock.MockOpenCypherDatabase;
import java.sql.SQLException;

public class OpenCypherStatementTest extends OpenCypherStatementTestBase {
    private static MockOpenCypherDatabase database;
    private static java.sql.Statement statement;

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() throws SQLException {
        database = MockOpenCypherDatabase.builder(HOSTNAME, OpenCypherStatementTest.class.getName()).build();
        PROPERTIES.putIfAbsent(ConnectionProperties.ENDPOINT_KEY, String.format("bolt://%s:%d", HOSTNAME, database.getPort()));
        final java.sql.Connection connection = new OpenCypherConnection(new ConnectionProperties(PROPERTIES));
        statement = connection.createStatement();
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
        launchCancelThread(0, statement);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }

    @Test
    void testCancelQueryWhileExecuteInProgress() {
        // Wait 200 milliseconds before attempting to cancel.
        launchCancelThread(200, statement);
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED, () -> statement.execute(getLongQuery()));
        waitCancelToComplete();
    }

    @Test
    void testCancelQueryTwice() {
        // Wait 200 milliseconds before attempting to cancel.
        launchCancelThread(200, statement);
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED, () -> statement.execute(getLongQuery()));
        waitCancelToComplete();
        launchCancelThread(1, statement);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }

    @Test
    void testCancelQueryAfterExecuteComplete() {
        Assertions.assertDoesNotThrow(() -> statement.execute(QUICK_QUERY));
        launchCancelThread(0, statement);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }
}
