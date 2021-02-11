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

import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.utilities.ConnectionProperties;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.concurrent.Executor;

/**
 * OpenCypher implementation of Connection.
 */
public class OpenCypherConnection extends software.amazon.jdbc.Connection implements java.sql.Connection {
    /**
     * OpenCypherConnection constructor, initializes super class.
     *
     * @param connectionProperties ConnectionProperties Object.
     */
    public OpenCypherConnection(@NonNull final ConnectionProperties connectionProperties) throws SQLException {
        super(connectionProperties);
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        final OpenCypherQueryExecutor queryExecutor =
                new OpenCypherQueryExecutor(new OpenCypherConnectionProperties(getConnectionProperties()));
        return queryExecutor.isValid(timeout);
    }

    @Override
    public void doClose() {
        // TODO.
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return new OpenCypherDatabaseMetadata(this);
    }

    @Override
    public int getNetworkTimeout() {
        // TODO.
        return 0;
    }

    @Override
    public void setNetworkTimeout(final Executor executor, final int milliseconds) {
        // TODO.
    }

    @Override
    public java.sql.Statement createStatement(final int resultSetType, final int resultSetConcurrency)
            throws SQLException {
        return new OpenCypherStatement(this);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(final String sql) throws SQLException {
        return new OpenCypherPreparedStatement(this, sql);
    }
}
