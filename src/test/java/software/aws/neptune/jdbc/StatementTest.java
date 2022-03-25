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

package software.aws.neptune.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.aws.neptune.jdbc.helpers.HelperFunctions;
import software.aws.neptune.jdbc.mock.MockConnection;
import software.aws.neptune.jdbc.mock.MockResultSet;
import software.aws.neptune.jdbc.mock.MockStatement;
import software.aws.neptune.opencypher.OpenCypherConnectionProperties;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * Test for abstract Statement Object.
 */
public class StatementTest {
    private java.sql.Statement statement;
    private java.sql.Connection connection;

    @BeforeEach
    void initialize() throws SQLException {
        connection = new MockConnection(new OpenCypherConnectionProperties());
        statement = new MockStatement(connection);
    }

    @Test
    void testSetGetIs() {
        HelperFunctions.expectFunctionThrows(() -> statement.setPoolable(false));
        HelperFunctions.expectFunctionThrows(() -> statement.setCursorName(""));
        HelperFunctions.expectFunctionThrows(() -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.setFetchDirection(ResultSet.FETCH_FORWARD));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.setEscapeProcessing(false));
        HelperFunctions.expectFunctionThrows(() -> statement.setFetchSize(0));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.setFetchSize(1));
        HelperFunctions.expectFunctionThrows(() -> statement.setLargeMaxRows(-1));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.setLargeMaxRows(1));
        HelperFunctions.expectFunctionThrows(() -> statement.setMaxFieldSize(-1));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.setMaxFieldSize(1));
        HelperFunctions.expectFunctionThrows(() -> statement.setMaxRows(-1));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.setMaxRows(1));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getConnection(), connection);
        HelperFunctions
                .expectFunctionDoesntThrow(() -> statement.getFetchDirection(), java.sql.ResultSet.FETCH_FORWARD);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getFetchSize(), 0);
        HelperFunctions.expectFunctionThrows(() -> statement.getGeneratedKeys());
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getLargeMaxRows(), (long) 1);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getMaxFieldSize(), 1);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getLargeUpdateCount(), (long) -1);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getMaxRows(), 1);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getMoreResults(), false);
        ((MockStatement) statement).setResultSet(new MockResultSet(statement));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.execute(""));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getMoreResults(
                java.sql.Statement.CLOSE_CURRENT_RESULT), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getResultSet(), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getResultSetConcurrency(),
                java.sql.ResultSet.CONCUR_READ_ONLY);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getResultSetHoldability(),
                java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getResultSetType(),
                java.sql.ResultSet.TYPE_FORWARD_ONLY);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getUpdateCount(), -1);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.closeOnCompletion());
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.isCloseOnCompletion(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.isPoolable(), false);

        HelperFunctions.expectFunctionDoesntThrow(() -> statement.setLargeMaxRows(Long.MAX_VALUE));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getMaxRows(), Integer.MAX_VALUE);
    }

    @Test
    void testExecute() {
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.execute(""), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.execute("", 0), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.execute("", new int[] {}), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.execute("", new String[] {}), true);
        HelperFunctions.expectFunctionThrows(() -> statement.executeBatch());
        HelperFunctions.expectFunctionThrows(() -> statement.executeLargeBatch());
        HelperFunctions.expectFunctionThrows(() -> statement.executeLargeUpdate(""));
        HelperFunctions.expectFunctionThrows(() -> statement.executeLargeUpdate("", 0));
        HelperFunctions.expectFunctionThrows(() -> statement.executeLargeUpdate("", new int[] {}));
        HelperFunctions.expectFunctionThrows(() -> statement.executeLargeUpdate("", new String[] {}));
        HelperFunctions.expectFunctionThrows(() -> statement.executeUpdate(""));
        HelperFunctions.expectFunctionThrows(() -> statement.executeUpdate("", 0));
        HelperFunctions.expectFunctionThrows(() -> statement.executeUpdate("", new int[] {}));
        HelperFunctions.expectFunctionThrows(() -> statement.executeUpdate("", new String[] {}));
        HelperFunctions.expectFunctionThrows(() -> statement.executeBatch());
    }

    @Test
    void testMisc() {
        HelperFunctions.expectFunctionThrows(() -> statement.cancel());
        HelperFunctions.expectFunctionThrows(() -> statement.addBatch(""));
        HelperFunctions.expectFunctionThrows(() -> statement.clearBatch());
    }

    @Test
    void testClosed() {
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.isClosed(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.close());
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.isClosed(), true);
        HelperFunctions.expectFunctionThrows(() -> ((Statement) statement).verifyOpen());
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.close());
    }

    @Test
    void testResultSetClose() {
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.isClosed(), false);
        ((MockStatement) statement).setResultSet(new MockResultSet(statement));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.execute(""));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.close());
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.isClosed(), true);
        HelperFunctions.expectFunctionThrows(() -> ((Statement) statement).verifyOpen());
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.close());
    }

    @Test
    void testWrap() {
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.isWrapperFor(MockStatement.class), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.isWrapperFor(MockConnection.class), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.isWrapperFor(null), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.unwrap(MockStatement.class), statement);
        HelperFunctions.expectFunctionThrows(() -> statement.unwrap(MockConnection.class));
    }

    @Test
    void testWarnings() {
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getWarnings(), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.clearWarnings());
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getWarnings(), null);

        HelperFunctions
                .expectFunctionDoesntThrow(() -> ((Statement) statement).addWarning(HelperFunctions.getNewWarning1()));
        final SQLWarning warning = HelperFunctions.getNewWarning1();
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getWarnings(), warning);
        warning.setNextWarning(HelperFunctions.getNewWarning2());
        HelperFunctions
                .expectFunctionDoesntThrow(() -> ((Statement) statement).addWarning(HelperFunctions.getNewWarning2()));
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getWarnings(), warning);

        HelperFunctions.expectFunctionDoesntThrow(() -> statement.clearWarnings());
        HelperFunctions.expectFunctionDoesntThrow(() -> statement.getWarnings(), null);
    }
}
