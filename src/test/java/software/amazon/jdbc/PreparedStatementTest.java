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

package software.amazon.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.mock.MockConnection;
import software.amazon.jdbc.mock.MockPreparedStatement;
import software.amazon.jdbc.utilities.ConnectionProperties;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Test for abstract PreparedStatement Object.
 */
public class PreparedStatementTest {
    private java.sql.Connection connection;
    private java.sql.PreparedStatement preparedStatement;

    @BeforeEach
    void initialize() throws SQLException {
        connection = new MockConnection(new ConnectionProperties(new Properties()));
        preparedStatement = new MockPreparedStatement(connection, "");
    }

    @Test
    void testExecute() {
        HelperFunctions.expectFunctionDoesntThrow(() -> preparedStatement.execute(), true);
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.execute(""));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.executeQuery(""));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.executeUpdate());
    }

    @Test
    void testMisc() {
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.addBatch());
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.clearParameters());
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.getParameterMetaData());
    }

    @Test
    void testSet() {
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setArray(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setAsciiStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setAsciiStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setAsciiStream(0, null, (long)0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setAsciiStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setBigDecimal(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setBinaryStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setBinaryStream(0, null, (long)0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setBinaryStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setBlob(0, (Blob)null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setBlob(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setBlob(0, (InputStream)null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setBoolean(0, false));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setByte(0, (byte)0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setBytes(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setCharacterStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setCharacterStream(0, null, (long)0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setCharacterStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setClob(0, (Clob)null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setClob(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setClob(0, (Reader)null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setDate(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setDate(0, null, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setDouble(0, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setFloat(0, (float)0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setInt(0, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setLong(0, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setNCharacterStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setNCharacterStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setNClob(0, (NClob)null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setNClob(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setNClob(0, (Reader)null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setNString(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setNull(0, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setNull(0, 0, ""));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setObject(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setObject(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setObject(0, null, 0, 0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setRef(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setRowId(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setSQLXML(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setShort(0, (short)0));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setString(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setTime(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setTime(0, null, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setTimestamp(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setTimestamp(0, null, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setURL(0, null));
        HelperFunctions.expectFunctionThrows(() -> preparedStatement.setUnicodeStream(0, null, 0));
    }
}
