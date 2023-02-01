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

package software.aws.neptune.sparql;

import software.aws.neptune.jdbc.PooledConnection;
import java.sql.SQLException;

/**
 * Sparql implementation of PooledConnection.
 */
public class SparqlPooledConnection extends PooledConnection implements javax.sql.PooledConnection {

    /**
     * SparqlPooledConnection constructor, initializes super class.
     *
     * @param connection Connection Object.
     */
    public SparqlPooledConnection(final java.sql.Connection connection) {
        super(connection);
    }

    @Override
    public java.sql.Connection getConnection() throws SQLException {
        return new SparqlConnection(new SparqlConnectionProperties());
    }
}
