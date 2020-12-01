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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import java.util.LinkedList;
import java.util.List;

/**
 * Abstract implementation of PooledConnection for JDBC Driver.
 */
public abstract class PooledConnection implements javax.sql.PooledConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(PooledConnection.class);
    private final List<ConnectionEventListener> connectionEventListeners = new LinkedList<>();
    private final java.sql.Connection connection;

    /**
     * PooledConnection constructor.
     * @param connection Connection Object.
     */
    public PooledConnection(final java.sql.Connection connection) {
        this.connection = connection;
    }


    @Override
    public void close() {
        LOGGER.debug("Notify all connection listeners this PooledConnection object is closed.");
        final ConnectionEvent event = new ConnectionEvent(this, null);
        connectionEventListeners.forEach(l -> l.connectionClosed(event));
    }

    @Override
    public void addConnectionEventListener(final ConnectionEventListener listener) {
        LOGGER.debug("Add a ConnectionEventListener to this PooledConnection.");
        connectionEventListeners.add(listener);
    }

    @Override
    public void removeConnectionEventListener(final ConnectionEventListener listener) {
        LOGGER.debug("Remove the ConnectionEventListener attached to this PooledConnection.");
        connectionEventListeners.remove(listener);
    }

    @Override
    public void addStatementEventListener(final StatementEventListener listener) {
        // Do nothing, statement pooling is not supported.
        LOGGER.debug("addStatementEventListener is called on the current PooledConnection object.");
    }

    @Override
    public void removeStatementEventListener(final StatementEventListener listener) {
        // Do nothing, statement pooling is not supported.
        LOGGER.debug("removeStatementEventListener is called on the current PooledConnection object.");
    }
}
