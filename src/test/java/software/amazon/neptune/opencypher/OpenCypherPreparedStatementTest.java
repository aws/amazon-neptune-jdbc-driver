/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
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
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.neptune.opencypher.mock.MockOpenCypherDatabase;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

public class OpenCypherPreparedStatementTest extends OpenCypherStatementTestBase {
    private static final String HOSTNAME = "localhost";
    private static final Properties PROPERTIES = new Properties();
    private static MockOpenCypherDatabase database;
    private static java.sql.PreparedStatement openCypherPreparedStatement;
    private static java.sql.PreparedStatement openCypherPreparedStatementLongQuery;
    private static java.sql.PreparedStatement openCypherPreparedStatementQuickQuery;

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() throws SQLException {
        database = MockOpenCypherDatabase.builder(HOSTNAME, OpenCypherPreparedStatementTest.class.getName()).build();
        PROPERTIES.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reverse default to None
        PROPERTIES.putIfAbsent(OpenCypherConnectionProperties.ENDPOINT_KEY,
                String.format("bolt://%s:%d", HOSTNAME, database.getPort()));
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        database.shutdown();
    }

    @BeforeEach
    void initialize() throws SQLException {
        final java.sql.Connection connection = new OpenCypherConnection(new OpenCypherConnectionProperties(PROPERTIES));
        openCypherPreparedStatement = connection.prepareStatement("");
        openCypherPreparedStatementLongQuery = connection.prepareStatement(getLongQuery());
        openCypherPreparedStatementQuickQuery = connection.prepareStatement(QUICK_QUERY);
    }

    @Test
    void testCancelQueryWithoutExecute() {
        launchCancelThread(0, openCypherPreparedStatementLongQuery);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }

    @Test
    void testCancelQueryWhileExecuteInProgress() {
        // Wait 200 milliseconds before attempting to cancel.
        launchCancelThread(200, openCypherPreparedStatementLongQuery);
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED,
                () -> openCypherPreparedStatementLongQuery.execute());
        waitCancelToComplete();
    }

    @Test
    void testCancelQueryTwice() {
        // Wait 200 milliseconds before attempting to cancel.
        launchCancelThread(200, openCypherPreparedStatementLongQuery);
        HelperFunctions
                .expectFunctionThrows(SqlError.QUERY_CANCELED, () -> openCypherPreparedStatementLongQuery.execute());
        waitCancelToComplete();
        launchCancelThread(1, openCypherPreparedStatementLongQuery);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }

    @Test
    void testCancelQueryAfterExecuteComplete() {
        Assertions.assertDoesNotThrow(() -> openCypherPreparedStatementQuickQuery.execute());
        launchCancelThread(0, openCypherPreparedStatementQuickQuery);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED_OR_COMPLETE, this::getCancelException);
    }

    @Test
    void testExecuteUpdate() {
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.executeUpdate());
    }

    @Test
    void testMisc() {
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.addBatch());
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.clearParameters());
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.getParameterMetaData());
    }

    @Test
    void testSet() {
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setArray(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setAsciiStream(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setAsciiStream(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setAsciiStream(0, null, (long) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setAsciiStream(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setBigDecimal(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setBinaryStream(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setBinaryStream(0, null, (long) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setBinaryStream(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setBlob(0, (Blob) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setBlob(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setBlob(0, (InputStream) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setBoolean(0, false));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setByte(0, (byte) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setBytes(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setCharacterStream(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setCharacterStream(0, null, (long) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setCharacterStream(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setClob(0, (Clob) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setClob(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setClob(0, (Reader) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setDate(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setDate(0, null, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setDouble(0, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setFloat(0, (float) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setInt(0, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setLong(0, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setNCharacterStream(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setNCharacterStream(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setNClob(0, (NClob) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setNClob(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setNClob(0, (Reader) null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setNString(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setNull(0, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setNull(0, 0, ""));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setObject(0, null, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setObject(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setObject(0, null, 0, 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setRef(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setRowId(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setSQLXML(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setShort(0, (short) 0));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setString(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setTime(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setTime(0, null, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setTimestamp(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setTimestamp(0, null, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setURL(0, null));
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> openCypherPreparedStatement.setUnicodeStream(0, null, 0));
    }
}
