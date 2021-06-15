package software.amazon.neptune;

import lombok.AllArgsConstructor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.utilities.SqlError;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

@AllArgsConstructor
public class NeptunePreparedStatementTestHelper extends NeptuneStatementTestHelperBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(NeptunePreparedStatementTestHelper.class);
    private final java.sql.PreparedStatement preparedStatement;
    private final java.sql.PreparedStatement preparedStatementLongQuery;
    private final java.sql.PreparedStatement preparedStatementQuickQuery;

    /**
     * Function to close the statements.
     */
    public void close() throws SQLException {
        preparedStatement.close();
        preparedStatementLongQuery.close();
        preparedStatementQuickQuery.close();
    }

    /**
     * Function to test cancelling queries without executing first.
     */
    public void testCancelQueryWithoutExecute() {
        launchCancelThread(0, preparedStatement);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }

    /**
     * Function to test cancelling query while execution in progress.
     */
    public void testCancelQueryWhileExecuteInProgress() {
        // Wait 100 milliseconds before attempting to cancel.
        launchCancelThread(100, preparedStatementLongQuery);
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED, preparedStatementLongQuery::execute);
        waitCancelToComplete();
    }

    /**
     * Function to test cancelling query twice.
     */
    public void testCancelQueryTwice() {
        // Wait 100 milliseconds before attempting to cancel.
        launchCancelThread(100, preparedStatementLongQuery);
        HelperFunctions
                .expectFunctionThrows(SqlError.QUERY_CANCELED, preparedStatementLongQuery::execute);
        waitCancelToComplete();
        launchCancelThread(1, preparedStatementLongQuery);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }

    /**
     * Function to test cancelling query after execution is complete.
     */
    public void testCancelQueryAfterExecuteComplete() {
        Assertions.assertDoesNotThrow((ThrowingSupplier<Boolean>) preparedStatementQuickQuery::execute);
        launchCancelThread(0, preparedStatementQuickQuery);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }

    /**
     * Function to test misc functions.
     */
    public void testMisc() {
        Assertions.assertThrows(SQLFeatureNotSupportedException.class, preparedStatement::executeUpdate);
        Assertions.assertThrows(SQLFeatureNotSupportedException.class, preparedStatement::addBatch);
        Assertions.assertThrows(SQLFeatureNotSupportedException.class, preparedStatement::clearParameters);
        Assertions.assertThrows(SQLFeatureNotSupportedException.class, preparedStatement::getParameterMetaData);
    }

    /**
     * Function to test set functionality.
     */
    public void testSet() {
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setArray(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setAsciiStream(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setAsciiStream(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setAsciiStream(0, null, (long) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setAsciiStream(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setBigDecimal(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setBinaryStream(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setBinaryStream(0, null, (long) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setBinaryStream(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setBlob(0, (Blob) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setBlob(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setBlob(0, (InputStream) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setBoolean(0, false));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setByte(0, (byte) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setBytes(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setCharacterStream(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setCharacterStream(0, null, (long) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setCharacterStream(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setClob(0, (Clob) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setClob(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setClob(0, (Reader) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setDate(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setDate(0, null, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setDouble(0, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setFloat(0, (float) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setInt(0, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setLong(0, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setNCharacterStream(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setNCharacterStream(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setNClob(0, (NClob) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setNClob(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setNClob(0, (Reader) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setNString(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setNull(0, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setNull(0, 0, ""));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setObject(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setObject(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setObject(0, null, 0, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setRef(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setRowId(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setSQLXML(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setShort(0, (short) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setString(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setTime(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setTime(0, null, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setTimestamp(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setTimestamp(0, null, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setURL(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> preparedStatement.setUnicodeStream(0, null, 0));
    }
}

