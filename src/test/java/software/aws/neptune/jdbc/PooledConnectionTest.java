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

package software.aws.neptune.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.aws.neptune.jdbc.helpers.HelperFunctions;
import software.aws.neptune.jdbc.mock.MockConnection;
import software.aws.neptune.jdbc.mock.MockPooledConnection;
import software.aws.neptune.opencypher.OpenCypherConnectionProperties;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import java.sql.SQLException;

/**
 * Test for abstract PooledConnection Object.
 */
public class PooledConnectionTest {
    private javax.sql.PooledConnection pooledConnection;
    private boolean isClosed;
    private boolean isError;

    private final ConnectionEventListener listener = new ConnectionEventListener() {
        @Override
        public void connectionClosed(final ConnectionEvent event) {
            isClosed = true;
        }

        @Override
        public void connectionErrorOccurred(final ConnectionEvent event) {
            isError = true;
        }
    };

    @BeforeEach
    void initialize() throws SQLException {
        pooledConnection = new MockPooledConnection(new MockConnection(new OpenCypherConnectionProperties()));
        isClosed = false;
        isError = false;
    }

    @Test
    void testListeners() {
        pooledConnection.addConnectionEventListener(listener);
        Assertions.assertFalse(isClosed);
        Assertions.assertFalse(isError);
        HelperFunctions.expectFunctionDoesntThrow(() -> pooledConnection.close());
        Assertions.assertTrue(isClosed);
        Assertions.assertFalse(isError);
        pooledConnection.removeConnectionEventListener(listener);
        isClosed = false;
        HelperFunctions.expectFunctionDoesntThrow(() -> pooledConnection.close());
        Assertions.assertFalse(isClosed);
        Assertions.assertFalse(isError);

        pooledConnection.addStatementEventListener(null);
        pooledConnection.removeStatementEventListener(null);
    }
}
