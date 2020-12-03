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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.mock.MockConnection;
import software.amazon.jdbc.mock.MockStatement;
import software.amazon.jdbc.utilities.ConnectionProperty;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Test for abstract Connection Object.
 */
public class ConnectionTest {
    private java.sql.Connection connection;

    private static final String TEST_SCHEMA = "schema";
    private static final String TEST_CATALOG = "catalog";
    private static final String TEST_NATIVE_SQL = "native sql";
    private static final String TEST_PROP_KEY_UNSUPPORTED = "unsupported";
    private static final String TEST_PROP_VAL_UNSUPPORTED = "unsupported";
    private static final String TEST_PROP_KEY = ConnectionProperty.APPLICATION_NAME.getConnectionProperty();
    private static final String TEST_PROP_VAL = Driver.APPLICATION_NAME;
    private static final Properties TEST_PROP = new Properties();
    private static final Properties TEST_PROP_EMPTY = new Properties();
    private static final Map<String, Class<?>> TEST_TYPE_MAP =
            new ImmutableMap.Builder<String, Class<?>>().put("String", String.class).build();

    @BeforeEach
    void initialize() {
        connection = new MockConnection(new Properties());
        TEST_PROP.setProperty(TEST_PROP_KEY, TEST_PROP_VAL);
        TEST_PROP.setProperty(ConnectionProperty.APPLICATION_NAME.getConnectionProperty(), Driver.APPLICATION_NAME);
        TEST_PROP_EMPTY.setProperty(ConnectionProperty.APPLICATION_NAME.getConnectionProperty(), Driver.APPLICATION_NAME);
    }

    @Test
    void testTransactions() {
        // Transaction isolation.
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setTransactionIsolation(Connection.TRANSACTION_NONE));
        HelperFunctions.expectFunctionThrows(() -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
        HelperFunctions.expectFunctionThrows(() -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED));
        HelperFunctions.expectFunctionThrows(() -> connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ));
        HelperFunctions.expectFunctionThrows(() -> connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getTransactionIsolation(), Connection.TRANSACTION_NONE);

        // Savepoint.
        HelperFunctions.expectFunctionThrows(() -> connection.setSavepoint());
        HelperFunctions.expectFunctionThrows(() -> connection.setSavepoint(null));
        HelperFunctions.expectFunctionThrows(() -> connection.releaseSavepoint(null));

        // Rollback.
        HelperFunctions.expectFunctionThrows(() -> connection.rollback(null));
        HelperFunctions.expectFunctionThrows(() -> connection.rollback());

        // Commit.
        HelperFunctions.expectFunctionThrows(() -> connection.commit());

        // Abort.
        HelperFunctions.expectFunctionThrows(() -> connection.abort(null));

        // Holdability.
        HelperFunctions.expectFunctionThrows(() -> connection.setHoldability(0));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getHoldability(), ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Test
    void testStatements() {
        // Statement without transaction.
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.createStatement(), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.createStatement(0, 0), null);

        // Statement with transaction.
        HelperFunctions.expectFunctionThrows(() -> connection.createStatement(0, 0, 0));

        // Prepared statements.
        HelperFunctions.expectFunctionThrows(() -> connection.prepareStatement(null));
        HelperFunctions.expectFunctionThrows(() -> connection.prepareStatement(null, 0));
        HelperFunctions.expectFunctionThrows(() -> connection.prepareStatement(null, 0, 0));
        HelperFunctions.expectFunctionThrows(() -> connection.prepareStatement(null, 0, 0, 0));
        HelperFunctions.expectFunctionThrows(() -> connection.prepareStatement(null, new int[] {}));
        HelperFunctions.expectFunctionThrows(() -> connection.prepareStatement(null, new String[] {}));

        // Callable statements.
        HelperFunctions.expectFunctionThrows(() -> connection.prepareCall(null));
        HelperFunctions.expectFunctionThrows(() -> connection.prepareCall(null, 0, 0));
        HelperFunctions.expectFunctionThrows(() -> connection.prepareCall(null, 0, 0, 0));
    }

    @Test
    void testClientInfo() {
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(), TEST_PROP_EMPTY);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(null), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setClientInfo(TEST_PROP_KEY, TEST_PROP_VAL));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(TEST_PROP_KEY), TEST_PROP_VAL);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(null), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setClientInfo(null));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(), TEST_PROP_EMPTY);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setClientInfo(TEST_PROP));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(), TEST_PROP);

        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setClientInfo(null));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setClientInfo(TEST_PROP_KEY, null));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(), TEST_PROP_EMPTY);

        HelperFunctions.expectFunctionDoesntThrow(() -> connection.clearWarnings());
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setClientInfo(TEST_PROP_KEY_UNSUPPORTED, TEST_PROP_VAL_UNSUPPORTED));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getWarnings(), HelperFunctions.TEST_SQL_WARNING_UNSUPPORTED);

        HelperFunctions.expectFunctionDoesntThrow(() -> connection.close());
        HelperFunctions.expectFunctionThrows(() -> connection.setClientInfo(TEST_PROP_KEY, TEST_PROP_VAL));
        HelperFunctions.expectFunctionThrows(() -> connection.setClientInfo(TEST_PROP));
    }

    @Test
    void testDataTypes() {
        HelperFunctions.expectFunctionThrows(() -> connection.createBlob());
        HelperFunctions.expectFunctionThrows(() -> connection.createClob());
        HelperFunctions.expectFunctionThrows(() -> connection.createNClob());
        HelperFunctions.expectFunctionThrows(() -> connection.createSQLXML());
        HelperFunctions.expectFunctionThrows(() -> connection.createArrayOf(null, new Object[]{}));
        HelperFunctions.expectFunctionThrows(() -> connection.createStruct(null, new Object[]{}));
    }

    @Test
    void testCatalog() {
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getCatalog(), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setCatalog(TEST_CATALOG));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getCatalog(), null);

    }

    @Test
    void testSchema() {
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getSchema(), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getSchema(), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setSchema(TEST_SCHEMA));
    }

    @Test
    void testWarnings() {
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getWarnings(), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.clearWarnings());
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getWarnings(), null);

        HelperFunctions.expectFunctionDoesntThrow(() -> ((Connection)connection).addWarning(HelperFunctions.getNewWarning1()));
        final SQLWarning warning = HelperFunctions.getNewWarning1();
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getWarnings(), warning);
        warning.setNextWarning(HelperFunctions.getNewWarning2());
        HelperFunctions.expectFunctionDoesntThrow(() -> ((Connection)connection).addWarning(HelperFunctions.getNewWarning2()));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getWarnings(), warning);

        HelperFunctions.expectFunctionDoesntThrow(() -> connection.clearWarnings());
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getWarnings(), null);
    }

    @Test
    void testReadOnly() {
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setReadOnly(true));
        HelperFunctions.expectFunctionThrows(() -> connection.setReadOnly(false));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.isReadOnly(), true);
    }

    @Test
    void testAutoCommit() {
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setAutoCommit(true));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setAutoCommit(false));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getAutoCommit(), true);
    }

    @Test
    void testClosed() {
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.isClosed(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.close());
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.isClosed(), true);
        HelperFunctions.expectFunctionThrows(() -> ((Connection)connection).verifyOpen());
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.close());
    }

    @Test
    void testWrap() {
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.isWrapperFor(MockConnection.class), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.isWrapperFor(MockStatement.class), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.isWrapperFor(null), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.unwrap(MockConnection.class), connection);
        HelperFunctions.expectFunctionThrows(() -> connection.unwrap(MockStatement.class));
    }

    @Test
    void testTypeMap() {
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getTypeMap(), new HashMap<>());
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setTypeMap(TEST_TYPE_MAP));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getTypeMap(), TEST_TYPE_MAP);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setTypeMap(null));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getTypeMap(), new HashMap<>());
    }

    @Test
    void testNativeSQL() {
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.nativeSQL(TEST_NATIVE_SQL), TEST_NATIVE_SQL);
    }
}
