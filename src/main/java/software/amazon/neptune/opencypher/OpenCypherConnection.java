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

import lombok.Getter;
import lombok.NonNull;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.jdbc.utilities.QueryExecutor;
import software.amazon.neptune.NeptuneDatabaseMetadata;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * OpenCypher implementation of Connection.
 */
public class OpenCypherConnection extends software.amazon.jdbc.Connection implements java.sql.Connection {
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
    public QueryExecutor getQueryExecutor() {
        return new OpenCypherQueryExecutor(getOpenCypherConnectionProperties());
    }

    @Override
    public String getDriverName() {
        return "neptune:opencypher";
    }
}
