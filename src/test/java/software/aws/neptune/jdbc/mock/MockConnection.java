/*
 * Copyright <2023> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.jdbc.mock;

import lombok.NonNull;
import software.aws.neptune.jdbc.Connection;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import software.aws.neptune.jdbc.utilities.QueryExecutor;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Mock implementation for Connection object so it can be instantiated and tested.
 */
public class MockConnection extends Connection implements java.sql.Connection {

    /**
     * Constructor for MockConnection.
     *
     * @param connectionProperties Properties to pass to Connection.
     */
    public MockConnection(
            final @NonNull ConnectionProperties connectionProperties) throws SQLException {
        super(connectionProperties);
    }

    @Override
    public QueryExecutor getQueryExecutor() {
        return new MockQueryExecutor();
    }

    @Override
    protected void doClose() {
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }
}
