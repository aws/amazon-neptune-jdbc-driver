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

import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.neptune.opencypher.mock.MockOpenCypherDatabase;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.util.Properties;

public class OpenCypherPreparedStatementTest extends OpenCypherStatementTestBase {
    private static final String HOSTNAME = "localhost";
    private static final Properties PROPERTIES = new Properties();
    private static MockOpenCypherDatabase database;
    private java.sql.PreparedStatement openCypherPreparedStatement;
    private java.sql.PreparedStatement openCypherPreparedStatementLongQuery;
    private java.sql.PreparedStatement openCypherPreparedStatementQuickQuery;

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() {
        database = MockOpenCypherDatabase.builder(HOSTNAME, OpenCypherResultSetMetadataTest.class.getName()).build();
        PROPERTIES.putIfAbsent(ConnectionProperties.ENDPOINT_KEY, String.format("bolt://%s:%d", HOSTNAME, database.getPort()));
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        database.shutdown();
    }

    @SneakyThrows
    @BeforeEach
    void initialize() {
        PROPERTIES.putIfAbsent(ConnectionProperties.ENDPOINT_KEY, String.format("bolt://%s:%d", HOSTNAME, 999));
        final java.sql.Connection connection = new OpenCypherConnection(new ConnectionProperties(PROPERTIES));
        openCypherPreparedStatement = connection.prepareStatement("");
        openCypherPreparedStatementLongQuery = connection.prepareStatement(getLongQuery());
        openCypherPreparedStatementQuickQuery = connection.prepareStatement(QUICK_QUERY);
    }

    @Test
    void testCancelQueryWithoutExecute() {
        launchCancelThread(0, openCypherPreparedStatementLongQuery);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_NOT_STARTED, this::getCancelException);
    }

    @Test
    void testCancelQueryWhileExecuteInProgress() {
        // Wait 1 second before attempting to cancel.
        launchCancelThread(1000, openCypherPreparedStatementLongQuery);
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED,
                () -> openCypherPreparedStatementLongQuery.execute());
        waitCancelToComplete();
    }

    @Test
    void testCancelQueryTwice() {
        // Wait 1 second before attempting to cancel.
        launchCancelThread(1000, openCypherPreparedStatementLongQuery);
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED,
                () -> openCypherPreparedStatementLongQuery.execute());
        waitCancelToComplete();
        launchCancelThread(0, openCypherPreparedStatementLongQuery);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANCELED, this::getCancelException);
    }

    @Test
    void testCancelQueryAfterExecuteComplete() {
        Assertions.assertDoesNotThrow(() -> openCypherPreparedStatementQuickQuery.execute());
        launchCancelThread(0, openCypherPreparedStatementQuickQuery);
        waitCancelToComplete();
        HelperFunctions.expectFunctionThrows(SqlError.QUERY_CANNOT_BE_CANCELLED, this::getCancelException);
    }

    @Test
    void testExecuteUpdate() {
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.executeUpdate());
    }

    @Test
    void testMisc() {
        HelperFunctions
                .expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED, () -> openCypherPreparedStatement.addBatch());
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.clearParameters());
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.getParameterMetaData());
    }

    @Test
    void testSet() {
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setArray(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setAsciiStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setAsciiStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setAsciiStream(0, null, (long) 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setAsciiStream(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setBigDecimal(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setBinaryStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setBinaryStream(0, null, (long) 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setBinaryStream(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setBlob(0, (Blob) null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setBlob(0, null, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setBlob(0, (InputStream) null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setBoolean(0, false));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setByte(0, (byte) 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setBytes(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setCharacterStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setCharacterStream(0, null, (long) 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setCharacterStream(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setClob(0, (Clob) null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setClob(0, null, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setClob(0, (Reader) null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setDate(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setDate(0, null, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setDouble(0, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setFloat(0, (float) 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setInt(0, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setLong(0, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setNCharacterStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setNCharacterStream(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setNClob(0, (NClob) null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setNClob(0, null, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setNClob(0, (Reader) null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setNString(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setNull(0, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setNull(0, 0, ""));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setObject(0, null, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setObject(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setObject(0, null, 0, 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setRef(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setRowId(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setSQLXML(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setShort(0, (short) 0));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setString(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setTime(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setTime(0, null, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setTimestamp(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setTimestamp(0, null, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setURL(0, null));
        HelperFunctions.expectFunctionThrows(SqlError.PARAMETERS_NOT_SUPPORTED,
                () -> openCypherPreparedStatement.setUnicodeStream(0, null, 0));
    }
}
