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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.neptune.NeptuneConstants;
import software.amazon.neptune.opencypher.mock.MockOpenCypherDatabase;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OpenCypherStatementTestBase {
    protected static final String HOSTNAME = "localhost";
    protected static final Properties PROPERTIES = new Properties();
    protected static final String LONG_QUERY;
    protected static final String QUICK_QUERY;
    protected static final int LONG_QUERY_NODE_COUNT = 1000;
    protected static MockOpenCypherDatabase database;
    static {
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < LONG_QUERY_NODE_COUNT; i++ ) {
            stringBuilder.append(String.format("CREATE (node%d:Foo) ", i));
        }
        stringBuilder.append("RETURN ");
        for (int i = 0; i < LONG_QUERY_NODE_COUNT; i++ ) {
            if (i != 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(String.format("node%d", i));
        }
        LONG_QUERY = stringBuilder.toString();
        QUICK_QUERY = "CREATE (quick:Foo) RETURN quick";
    }
    protected java.sql.Statement statement;
    protected final ExecutorService cancelThread = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("cancelThread").setDaemon(true).build());
    protected OpenCypherStatementTest.Cancel cancel = null;

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() {
        database = MockOpenCypherDatabase.builder(HOSTNAME, OpenCypherResultSetMetadataTest.class.getName()).build();
        PROPERTIES.putIfAbsent(NeptuneConstants.ENDPOINT, String.format("bolt://%s:%d", HOSTNAME, database.getPort()));
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        database.shutdown();
    }

//    @Test
//    void testCancelQueryWithoutExecute() {
//        launchCancelThread(0);
//        waitCancelToComplete();
//        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED, () -> cancel.getException());
//    }
//
//    @Test
//    void testCancelQueryWhileExecuteInProgress() {
//        // Wait 1 second before attempting to cancel.
//        launchCancelThread(500);
//        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED, () -> statement.execute(LONG_QUERY));
//        waitCancelToComplete();
//    }
//
//    @Test
//    void testCancelQueryTwice() {
//        // Wait 1 second before attempting to cancel.
//        launchCancelThread(500);
//        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED, () -> statement.execute(LONG_QUERY));
//        waitCancelToComplete();
//        launchCancelThread(0);
//        waitCancelToComplete();
//        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED, () -> cancel.getException());
//    }
//
//    @Test
//    void testCancelQueryAfterExecuteComplete() {
//        Assertions.assertDoesNotThrow(() -> statement.execute(QUICK_QUERY));
//        launchCancelThread(0);
//        waitCancelToComplete();
//        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANNOT_BE_CANCELLED, () -> cancel.getException());
//    }

    public void launchCancelThread(final int waitTime) {
        //cancel = new Cancel(statement, waitTime);
        cancelThread.execute(cancel);
    }

    @SneakyThrows
    public void waitCancelToComplete() {
        cancelThread.awaitTermination(1000, TimeUnit.MILLISECONDS);
    }

    /**
     * Class to cancel query in a separate thread.
     */
    public static class Cancel implements Runnable {
        private final Statement statement;
        private final int waitTime;
        private SQLException exception;

        Cancel(final Statement statement, final int waitTime) {
            this.statement = statement;
            this.waitTime = waitTime;
        }

        @SneakyThrows
        @Override
        public void run() {
            try {
                Thread.sleep(waitTime);
                statement.cancel();
            } catch (final SQLException e) {
                exception = e;
            }
        }

        /**
         * Function to get exception if the run call generated one.
         * @throws SQLException Exception caught by run.
         */
        public void getException() throws SQLException {
            if (exception != null) {
                throw exception;
            }
        }
    }
}
