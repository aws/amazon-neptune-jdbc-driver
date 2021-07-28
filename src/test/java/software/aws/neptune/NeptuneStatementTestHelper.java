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

package software.aws.neptune;

import lombok.AllArgsConstructor;
import org.junit.jupiter.api.Assertions;
import software.aws.neptune.jdbc.helpers.HelperFunctions;
import software.aws.neptune.jdbc.utilities.SqlError;

@AllArgsConstructor
public class NeptuneStatementTestHelper extends NeptuneStatementTestHelperBase {
    private final java.sql.Statement statement;
    private final String longQuery;
    private final String quickQuery;

    /**
     * Function to test cancelling queries without executing first.
     */
    public void testCancelQueryWithoutExecute() {
        launchCancelThread(0, statement);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }

    /**
     * Function to test cancelling query while execution in progress.
     */
    public void testCancelQueryWhileExecuteInProgress() {
        // Wait 100 milliseconds before attempting to cancel.
        launchCancelThread(100, statement);
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED, () -> statement.execute(longQuery));
        waitCancelToComplete();
    }

    /**
     * Function to test cancelling query twice.
     */
    public void testCancelQueryTwice() {
        // Wait 100 milliseconds before attempting to cancel.
        launchCancelThread(100, statement);
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED, () -> statement.execute(longQuery));
        waitCancelToComplete();
        launchCancelThread(1, statement);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }

    /**
     * Function to test cancelling query after execution is complete.
     */
    public void testCancelQueryAfterExecuteComplete() {
        Assertions.assertDoesNotThrow(() -> statement.execute(quickQuery));
        launchCancelThread(0, statement);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }
}
