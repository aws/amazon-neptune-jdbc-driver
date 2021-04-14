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
import software.amazon.jdbc.mock.MockResultSet;
import software.amazon.jdbc.mock.MockStatement;
import software.amazon.neptune.opencypher.OpenCypherConnectionProperties;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Map;

/**
 * Test for abstract ResultSet Object.
 */
public class ResultSetTest {
    private java.sql.ResultSet resultSet;
    private java.sql.Statement statement;

    @BeforeEach
    void initialize() throws SQLException {
        statement = new MockStatement(new MockConnection(new OpenCypherConnectionProperties()));
        resultSet = new MockResultSet(statement);
    }

    @Test
    void testGetType() {
        HelperFunctions.expectFunctionThrows(() -> resultSet.getArray(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getArray(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getAsciiStream(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getAsciiStream(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBigDecimal(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBigDecimal(0, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBigDecimal(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBigDecimal("", 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBinaryStream(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBinaryStream(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBlob(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getBlob(""));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getBoolean(0), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getBoolean(""), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getByte(0), (byte) 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getByte(""), (byte) 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getBytes(0), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getBytes(""), null);
        HelperFunctions.expectFunctionThrows(() -> resultSet.getCharacterStream(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getCharacterStream(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getClob(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getClob(""));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getDate(0), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getDate(""), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getDate(0, null), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getDate("", null), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getDouble(0), 0.0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getDouble(""), 0.0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getFloat(0), 0.0f);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getFloat(""), 0.0f);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getInt(0), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getInt(""), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getLong(0), 0L);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getLong(""), 0L);
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNCharacterStream(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNCharacterStream(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNClob(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNClob(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNString(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getNString(""));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getObject(0), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getObject(""), null);
        HelperFunctions.expectFunctionThrows(() -> resultSet.getObject(0, (Class<?>) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getObject("", (Class<?>) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getObject(0, (Map<String, Class<?>>) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getObject("", (Map<String, Class<?>>) null));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getObject(""), null);
        HelperFunctions.expectFunctionThrows(() -> resultSet.getRef(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getRef(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getRowId(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getRowId(""));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getShort(0), (short) 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getShort(""), (short) 0);
        HelperFunctions.expectFunctionThrows(() -> resultSet.getSQLXML(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getSQLXML(""));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getString(0), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getString(""), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getTime(0), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getTime(""), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getTime(0, null), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getTime("", null), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getTimestamp(0), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getTimestamp(""), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getTimestamp(0, null), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getTimestamp("", null), null);
        HelperFunctions.expectFunctionThrows(() -> resultSet.getUnicodeStream(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getUnicodeStream(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getURL(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.getURL(""));
    }

    @Test
    void testUpdate() {
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateArray(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateArray("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream(0, null, (long) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream("", null, (long) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateAsciiStream("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBigDecimal(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBigDecimal("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream(0, null, (long) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream("", null, (long) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBinaryStream("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob(0, (Blob) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob("", (Blob) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob(0, (InputStream) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBlob("", (InputStream) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBoolean(0, false));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBoolean("", false));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateByte(0, (byte) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateByte("", (byte) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBytes(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateBytes("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream(0, null, (long) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream("", null, (long) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateCharacterStream("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob(0, (Clob) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob("", (Clob) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob(0, (Reader) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateClob("", (Reader) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateDate(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateDate("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateDouble(0, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateDouble("", 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateFloat(0, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateFloat("", 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateInt(0, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateInt("", 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateLong(0, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateLong("", 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNCharacterStream(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNCharacterStream("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNCharacterStream(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNCharacterStream("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob(0, (NClob) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob("", (NClob) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob(0, (Reader) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNClob("", (Reader) null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNString(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNString("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNull(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateNull(""));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateObject(0, null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateObject(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateObject("", null, 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateObject("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateRef(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateRef("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateRowId(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateRowId("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateSQLXML(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateSQLXML("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateShort(0, (short) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateShort("", (short) 0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateString(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateString("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateTime(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateTime("", null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateTimestamp(0, null));
        HelperFunctions.expectFunctionThrows(() -> resultSet.updateTimestamp("", null));
    }

    @Test
    void testRow() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.rowDeleted(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.rowInserted(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.rowUpdated(), false);
        resultSet = new MockResultSet(statement);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.next(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.relative(1), true);
        resultSet = new MockResultSet(statement);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.next(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.relative(10), false);
        HelperFunctions.expectFunctionThrows(() -> resultSet.relative(-1));
        HelperFunctions.expectFunctionThrows(() -> resultSet.moveToCurrentRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.refreshRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.previous());
        HelperFunctions.expectFunctionThrows(() -> resultSet.insertRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.moveToInsertRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.deleteRow());
        HelperFunctions.expectFunctionThrows(() -> resultSet.cancelRowUpdates());
        resultSet = new MockResultSet(statement);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.next(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isAfterLast(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isBeforeFirst(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isFirst(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isLast(), false);
        resultSet = new MockResultSet(statement);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isBeforeFirst(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isFirst(), false);
        resultSet = new MockResultSet(statement);
        for (int i = 0; i < 10; i++) {
            HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.next(), true);
        }
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isLast(), true);
        resultSet = new MockResultSet(statement);
        for (int i = 0; i < 10; i++) {
            HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.next(), true);
        }
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.next(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isAfterLast(), true);
        resultSet = new MockResultSet(statement);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.next(), true);
        HelperFunctions.expectFunctionThrows(() -> resultSet.first());
        HelperFunctions.expectFunctionThrows(() -> resultSet.last());
        HelperFunctions.expectFunctionThrows(() -> resultSet.beforeFirst());
        HelperFunctions.expectFunctionThrows(() -> resultSet.afterLast());
        resultSet = new MockResultSet(statement);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.next(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getRow(), 1);
        HelperFunctions.expectFunctionThrows(() -> resultSet.absolute(-1));
        HelperFunctions.expectFunctionThrows(() -> resultSet.absolute(0));
        resultSet = new MockResultSet(statement);
        for (int i = 0; i < 10; i++) {
            HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.next(), true);
        }
        HelperFunctions.expectFunctionThrows(() -> resultSet.absolute(0));
        HelperFunctions.expectFunctionThrows(() -> resultSet.absolute(5));
        resultSet = new MockResultSet(statement);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.next(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.absolute(5), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.absolute(10), false);
    }

    @Test
    void testFetch() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.setFetchDirection(java.sql.ResultSet.FETCH_FORWARD));
        HelperFunctions.expectFunctionThrows(() -> resultSet.setFetchDirection(java.sql.ResultSet.FETCH_REVERSE));
        HelperFunctions.expectFunctionThrows(() -> resultSet.setFetchDirection(java.sql.ResultSet.FETCH_UNKNOWN));
        HelperFunctions
                .expectFunctionDoesntThrow(() -> resultSet.getFetchDirection(), java.sql.ResultSet.FETCH_FORWARD);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getType(), java.sql.ResultSet.TYPE_FORWARD_ONLY);
        HelperFunctions
                .expectFunctionDoesntThrow(() -> resultSet.getConcurrency(), java.sql.ResultSet.CONCUR_READ_ONLY);
        HelperFunctions.expectFunctionThrows(() -> resultSet.getCursorName());
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getFetchSize(), 0);
        HelperFunctions.expectFunctionThrows(() -> resultSet.setFetchSize(-1));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.setFetchSize(0));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.setFetchSize(1));
    }

    @Test
    void testGetStatement() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getStatement(), statement);
    }

    @Test
    void testWrap() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isWrapperFor(MockResultSet.class), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isWrapperFor(MockStatement.class), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isWrapperFor(null), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.unwrap(MockResultSet.class), resultSet);
        HelperFunctions.expectFunctionThrows(() -> resultSet.unwrap(MockStatement.class));
    }

    @Test
    void testClosed() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isClosed(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.close());
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.isClosed(), true);
        HelperFunctions.expectFunctionThrows(() -> ((ResultSet) resultSet).verifyOpen());
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.close());
    }

    @Test
    void testWarnings() {
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getWarnings(), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.clearWarnings());
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getWarnings(), null);

        HelperFunctions
                .expectFunctionDoesntThrow(() -> ((ResultSet) resultSet).addWarning(HelperFunctions.getNewWarning1()));
        final SQLWarning warning = HelperFunctions.getNewWarning1();
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getWarnings(), warning);
        warning.setNextWarning(HelperFunctions.getNewWarning2());
        HelperFunctions
                .expectFunctionDoesntThrow(() -> ((ResultSet) resultSet).addWarning(HelperFunctions.getNewWarning2()));
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getWarnings(), warning);

        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.clearWarnings());
        HelperFunctions.expectFunctionDoesntThrow(() -> resultSet.getWarnings(), null);
    }
}
