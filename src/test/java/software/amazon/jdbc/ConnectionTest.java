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
import org.apache.log4j.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.mock.MockConnection;
import software.amazon.jdbc.mock.MockStatement;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.neptune.opencypher.OpenCypherConnectionProperties;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import static software.amazon.jdbc.utilities.ConnectionProperties.APPLICATION_NAME_KEY;
import static software.amazon.jdbc.utilities.ConnectionProperties.LOG_LEVEL_KEY;

/**
 * Test for abstract Connection Object.
 */
public class ConnectionTest {
    private static final Properties PROPERTIES = new Properties();
    private static final String TEST_SCHEMA = "schema";
    private static final String TEST_CATALOG = "catalog";
    private static final String TEST_NATIVE_SQL = "native sql";
    private static final String TEST_PROP_KEY_UNSUPPORTED = "unsupported";
    private static final String TEST_PROP_VAL_UNSUPPORTED = "unsupported";
    private static final String TEST_PROP_KEY = LOG_LEVEL_KEY;
    private static final Level TEST_PROP_VAL = Level.INFO;
    private static final Properties TEST_PROP = new Properties();
    private static final Properties TEST_PROP_INITIAL = new Properties();
    private static final Properties TEST_PROP_MODIFIED = new Properties();
    private static final Map<String, Class<?>> TEST_TYPE_MAP =
            new ImmutableMap.Builder<String, Class<?>>().put("String", String.class).build();
    private java.sql.Connection connection;

    @BeforeEach
    void initialize() throws SQLException {
        PROPERTIES.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        connection = new MockConnection(new OpenCypherConnectionProperties(PROPERTIES));

        TEST_PROP.put(TEST_PROP_KEY, TEST_PROP_VAL);
        TEST_PROP_INITIAL.put(APPLICATION_NAME_KEY, Driver.APPLICATION_NAME);
        TEST_PROP_INITIAL.putAll(ConnectionProperties.DEFAULT_PROPERTIES_MAP);
        TEST_PROP_INITIAL.putAll(PROPERTIES);
        TEST_PROP_MODIFIED.putAll(TEST_PROP_INITIAL);
        TEST_PROP_MODIFIED.remove(TEST_PROP_KEY);
    }

    @Test
    void testTransactions() {
        // Transaction isolation.
        HelperFunctions
                .expectFunctionDoesntThrow(() -> connection.setTransactionIsolation(Connection.TRANSACTION_NONE));
        HelperFunctions
                .expectFunctionThrows(() -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
        HelperFunctions.expectFunctionThrows(
                () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED));
        HelperFunctions
                .expectFunctionThrows(() -> connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ));
        HelperFunctions
                .expectFunctionThrows(() -> connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
        HelperFunctions
                .expectFunctionDoesntThrow(() -> connection.getTransactionIsolation(), Connection.TRANSACTION_NONE);

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
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(), TEST_PROP_INITIAL);

        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setClientInfo(null));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(null), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(), TEST_PROP_INITIAL);

        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(), TEST_PROP_INITIAL);

        HelperFunctions.expectFunctionDoesntThrow(
                () -> connection.setClientInfo(TEST_PROP_KEY, String.valueOf(TEST_PROP_VAL)));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(TEST_PROP_KEY),
                String.valueOf(TEST_PROP_VAL));

        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setClientInfo(TEST_PROP_KEY, ""));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(), TEST_PROP_INITIAL);

        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setClientInfo(TEST_PROP));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(TEST_PROP_KEY),
                String.valueOf(TEST_PROP_VAL));

        HelperFunctions.expectFunctionDoesntThrow(() -> connection.setClientInfo(TEST_PROP_KEY, null));
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getClientInfo(), TEST_PROP_MODIFIED);

        HelperFunctions.expectFunctionThrows(
                SqlError.lookup(SqlError.INVALID_CONNECTION_PROPERTY, TEST_PROP_KEY_UNSUPPORTED, ""),
                () -> connection.setClientInfo(TEST_PROP_KEY_UNSUPPORTED, ""));
        HelperFunctions.expectFunctionThrows(
                SqlError.lookup(SqlError.INVALID_CONNECTION_PROPERTY, TEST_PROP_KEY, TEST_PROP_VAL_UNSUPPORTED),
                () -> connection.setClientInfo(TEST_PROP_KEY, TEST_PROP_VAL_UNSUPPORTED));

        HelperFunctions.expectFunctionDoesntThrow(() -> connection.close());
        HelperFunctions.expectFunctionThrows(
                SqlError.CONN_CLOSED,
                () -> connection.setClientInfo(TEST_PROP_KEY, String.valueOf(TEST_PROP_VAL)));
        HelperFunctions.expectFunctionThrows(
                SqlError.CONN_CLOSED,
                () -> connection.setClientInfo(TEST_PROP));
    }

    @Test
    void testDataTypes() {
        HelperFunctions.expectFunctionThrows(() -> connection.createBlob());
        HelperFunctions.expectFunctionThrows(() -> connection.createClob());
        HelperFunctions.expectFunctionThrows(() -> connection.createNClob());
        HelperFunctions.expectFunctionThrows(() -> connection.createSQLXML());
        HelperFunctions.expectFunctionThrows(() -> connection.createArrayOf(null, new Object[] {}));
        HelperFunctions.expectFunctionThrows(() -> connection.createStruct(null, new Object[] {}));
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

        HelperFunctions.expectFunctionDoesntThrow(
                () -> ((Connection) connection).addWarning(HelperFunctions.getNewWarning1()));
        final SQLWarning warning = HelperFunctions.getNewWarning1();
        HelperFunctions.expectFunctionDoesntThrow(() -> connection.getWarnings(), warning);
        warning.setNextWarning(HelperFunctions.getNewWarning2());
        HelperFunctions.expectFunctionDoesntThrow(
                () -> ((Connection) connection).addWarning(HelperFunctions.getNewWarning2()));
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
        HelperFunctions.expectFunctionThrows(() -> ((Connection) connection).verifyOpen());
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
