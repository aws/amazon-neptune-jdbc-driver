/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.neptune.gremlin;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.neptune.NeptuneStatementTestHelper;
import software.amazon.neptune.gremlin.mock.MockGremlinDatabase;
import java.io.IOException;
import java.sql.SQLException;
import static software.amazon.neptune.gremlin.GremlinHelper.getProperties;

public class GremlinStatementTest extends GremlinStatementTestBase {
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 8181;
    private static final int MAX_CONTENT_LENGTH = 500000; // Took from PropertyGraphSerializationModule.
    private static NeptuneStatementTestHelper neptuneStatementTestHelper;

    @BeforeEach
    void initialize() throws SQLException, IOException, InterruptedException {
        MockGremlinDatabase.startGraph();
        final java.sql.Connection connection =
                new GremlinConnection(
                        new GremlinConnectionProperties(getProperties(HOSTNAME, PORT, MAX_CONTENT_LENGTH)));
        neptuneStatementTestHelper =
                new NeptuneStatementTestHelper(connection.createStatement(), getLongQuery(), QUICK_QUERY);
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterEach
    void shutdownDatabase() throws IOException, InterruptedException {
        MockGremlinDatabase.stopGraph();
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
