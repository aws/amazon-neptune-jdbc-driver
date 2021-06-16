package software.amazon.neptune;

import lombok.AllArgsConstructor;
import org.junit.jupiter.api.Assertions;
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.utilities.SqlError;

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
