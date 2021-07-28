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

package software.aws.neptune;

import software.aws.neptune.jdbc.Connection;
import software.aws.neptune.jdbc.DatabaseMetaData;
import java.sql.SQLException;

public class NeptuneDatabaseMetadata extends DatabaseMetaData implements java.sql.DatabaseMetaData {
    private final Connection connection;

    /**
     * NeptuneDatabaseMetadata constructor, initializes super class.
     *
     * @param connection Connection Object.
     */
    public NeptuneDatabaseMetadata(final java.sql.Connection connection) {
        super(connection);
        this.connection = (Connection) connection;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Neptune";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "graph";
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return "-";
    }

    @Override
    public String getDriverName() {
        return connection.getDriverName();
    }
}
