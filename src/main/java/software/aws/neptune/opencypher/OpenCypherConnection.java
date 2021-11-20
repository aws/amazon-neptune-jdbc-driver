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

package software.aws.neptune.opencypher;

import lombok.Getter;
import lombok.NonNull;
import software.aws.neptune.NeptuneDatabaseMetadata;
import software.aws.neptune.jdbc.Connection;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import software.aws.neptune.jdbc.utilities.QueryExecutor;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * OpenCypher implementation of Connection.
 */
public class OpenCypherConnection extends Connection implements java.sql.Connection {
    @Getter
    private final OpenCypherConnectionProperties openCypherConnectionProperties;

    /**
     * OpenCypherConnection constructor, initializes super class.
     *
     * @param connectionProperties ConnectionProperties Object.
     */
    public OpenCypherConnection(@NonNull final ConnectionProperties connectionProperties) throws SQLException {
        super(connectionProperties);
        openCypherConnectionProperties = new OpenCypherConnectionProperties(getConnectionProperties());
    }

    @Override
    public void doClose() {
        OpenCypherQueryExecutor.close();
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return new NeptuneDatabaseMetadata(this);
    }

    @Override
    public QueryExecutor getQueryExecutor() throws SQLException {
        return new OpenCypherQueryExecutor(getOpenCypherConnectionProperties());
    }

    @Override
    public String getDriverName() {
        return "Neptune:openCypher";
    }
}
