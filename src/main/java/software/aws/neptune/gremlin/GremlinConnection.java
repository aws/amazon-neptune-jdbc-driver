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

package software.aws.neptune.gremlin;

import lombok.Getter;
import lombok.NonNull;
import software.aws.neptune.NeptuneDatabaseMetadata;
import software.aws.neptune.jdbc.Connection;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import software.aws.neptune.jdbc.utilities.QueryExecutor;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Gremlin implementation of Connection.
 */
public class GremlinConnection extends Connection implements java.sql.Connection {
    @Getter
    private final GremlinConnectionProperties gremlinConnectionProperties;

    /**
     * Gremlin constructor, initializes super class.
     *
     * @param connectionProperties ConnectionProperties Object.
     */
    public GremlinConnection(@NonNull final ConnectionProperties connectionProperties) throws SQLException {
        super(connectionProperties);
        this.gremlinConnectionProperties = new GremlinConnectionProperties(getConnectionProperties());
    }

    @Override
    public void doClose() {
        GremlinQueryExecutor.close();
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return new NeptuneDatabaseMetadata(this);
    }

    @Override
    public QueryExecutor getQueryExecutor() throws SQLException {
        return new GremlinQueryExecutor(getGremlinConnectionProperties());
    }

    @Override
    public String getDriverName() {
        return "Neptune:Gremlin";
    }
}
