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

package software.aws.neptune.gremlin;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.neptune.NeptunePreparedStatementTestHelper;
import software.aws.neptune.gremlin.mock.MockGremlinDatabase;
import java.io.IOException;
import java.sql.SQLException;

import static software.aws.neptune.gremlin.GremlinHelper.getProperties;

// TODO AN-887: Fix query cancellation issue and enable tests.
@Disabled
public class GremlinPreparedStatementTest extends GremlinStatementTestBase {
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 8181; // Mock server uses 8181.
    private static final int MAX_CONTENT_LENGTH = 500000; // Took from PropertyGraphSerializationModule.
    private NeptunePreparedStatementTestHelper neptunePreparedStatementTestHelper;

    @BeforeEach
    void initialize() throws SQLException, IOException, InterruptedException {
        MockGremlinDatabase.startGraph();
        final java.sql.Connection connection = new GremlinConnection(
                new GremlinConnectionProperties(getProperties(HOSTNAME, PORT, MAX_CONTENT_LENGTH)));
        neptunePreparedStatementTestHelper = new NeptunePreparedStatementTestHelper(connection.prepareStatement(""),
                connection.prepareStatement(getLongQuery()), connection.prepareStatement(QUICK_QUERY));
    }

    @AfterEach
    void close() throws SQLException, IOException, InterruptedException {
        neptunePreparedStatementTestHelper.close();
        MockGremlinDatabase.stopGraph();
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
