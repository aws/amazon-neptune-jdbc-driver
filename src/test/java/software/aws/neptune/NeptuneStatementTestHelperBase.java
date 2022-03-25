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

package software.aws.neptune;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.SneakyThrows;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NeptuneStatementTestHelperBase {

    @Getter
    private final ExecutorService cancelThread = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("cancelThread").setDaemon(true).build());
    private Cancel cancel = null;

    protected void launchCancelThread(final int waitTime, final Statement statement) {
        cancel = new Cancel(statement, waitTime);
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
