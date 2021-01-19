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
import lombok.Getter;
import lombok.SneakyThrows;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OpenCypherStatementTestBase {
    protected static final String HOSTNAME = "localhost";
    protected static final Properties PROPERTIES = new Properties();
    protected static final String QUICK_QUERY;
    protected static final int LONG_QUERY_NODE_COUNT = 500;
    private static int currentIndex = 0;

    static {
        QUICK_QUERY = "CREATE (quick:Foo) RETURN quick";
    }

    @Getter
    private final ExecutorService cancelThread = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("cancelThread").setDaemon(true).build());
    private Cancel cancel = null;

    static String getLongQuery() {
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = currentIndex; i < (currentIndex + LONG_QUERY_NODE_COUNT); i++) {
            stringBuilder.append(String.format("CREATE (node%d:Foo) ", i));
        }
        stringBuilder.append("RETURN ");
        for (int i = currentIndex; i < (currentIndex + LONG_QUERY_NODE_COUNT); i++) {
            if (i != currentIndex) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(String.format("node%d", i));
        }
        currentIndex += LONG_QUERY_NODE_COUNT;
        return stringBuilder.toString();
    }

    protected void launchCancelThread(final int waitTime, final Statement statement) {
        cancel = new OpenCypherStatementTestBase.Cancel(statement, waitTime);
        getCancelThread().execute(cancel);
    }

    protected void getCancelException() throws SQLException {
        cancel.getException();
    }

    @SneakyThrows
    protected void waitCancelToComplete() {
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
         *
         * @throws SQLException Exception caught by run.
         */
        public void getException() throws SQLException {
            if (exception != null) {
                throw exception;
            }
        }
    }
}
