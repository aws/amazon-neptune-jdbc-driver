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

package software.amazon.neptune.sparql;

import lombok.Getter;
import lombok.NonNull;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.jdbc.utilities.QueryExecutor;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class SparqlConnection extends software.amazon.jdbc.Connection implements java.sql.Connection {
    @Getter
    private final SparqlConnectionProperties sparqlConnectionProperties;

    /**
     * Sparql constructor, initializes super class.
     *
     * @param connectionProperties ConnectionProperties Object.
     */
    public SparqlConnection(@NonNull final ConnectionProperties connectionProperties) throws SQLException {
        super(connectionProperties);
        this.sparqlConnectionProperties = new SparqlConnectionProperties(getConnectionProperties());
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        final SparqlQueryExecutor queryExecutor = new SparqlQueryExecutor(sparqlConnectionProperties);
        return queryExecutor.isValid(timeout);
    }

    @Override
    protected QueryExecutor getQueryExecutor() {
        return new SparqlQueryExecutor(getSparqlConnectionProperties());
    }

    @Override
    protected void doClose() {

    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new SparqlDatabaseMetadata(this);
    }
}
