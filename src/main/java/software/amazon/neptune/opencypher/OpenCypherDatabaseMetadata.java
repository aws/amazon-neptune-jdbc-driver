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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.neptune.NeptuneDatabaseMetadata;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * OpenCypher implementation of DatabaseMetaData.
 */
public class OpenCypherDatabaseMetadata extends NeptuneDatabaseMetadata implements java.sql.DatabaseMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherDatabaseMetadata.class);
    private final OpenCypherConnection connection;

    /**
     * OpenCypherDatabaseMetadata constructor, initializes super class.
     *
     * @param connection Connection Object.
     */
    OpenCypherDatabaseMetadata(final java.sql.Connection connection) {
        super(connection);
        this.connection = (OpenCypherConnection) connection;
    }

    @Override
    public String getDriverName() throws SQLException {
        return "neptune:opencypher";
    }

    @Override
    public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern,
                               final String[] types)
            throws SQLException {
        // Only tableNamePattern is supported as an exact node label semicolon delimited String.
        LOGGER.info("Getting database tables.");
        final OpenCypherQueryExecutor openCypherQueryExecutor =
                new OpenCypherQueryExecutor(connection.getOpenCypherConnectionProperties());
        return openCypherQueryExecutor.executeGetTables(getConnection().createStatement(), tableNamePattern);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        LOGGER.info("Getting database schemas.");
        final OpenCypherQueryExecutor openCypherQueryExecutor =
                new OpenCypherQueryExecutor(connection.getOpenCypherConnectionProperties());
        return openCypherQueryExecutor.executeGetSchemas(getConnection().createStatement());
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        LOGGER.info("Getting database catalogs.");
        final OpenCypherQueryExecutor openCypherQueryExecutor =
                new OpenCypherQueryExecutor(connection.getOpenCypherConnectionProperties());
        return openCypherQueryExecutor.executeGetCatalogs(getConnection().createStatement());
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        LOGGER.info("Getting database table types.");
        final OpenCypherQueryExecutor openCypherQueryExecutor =
                new OpenCypherQueryExecutor(connection.getOpenCypherConnectionProperties());
        return openCypherQueryExecutor.executeGetTableTypes(getConnection().createStatement());
    }

    @Override
    public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
                                final String columnNamePattern)
            throws SQLException {
        if (catalog != null) {
            LOGGER.warn("Catalog in getColumns is not supported, ignoring.");
        }
        if (columnNamePattern != null) {
            LOGGER.warn("ColumnNamePattern in getColumns is not supported, ignoring.");
        }
        if (schemaPattern != null) {
            LOGGER.warn("SchemaPattern in getColumns is not supported, ignoring.");
        }
        final OpenCypherQueryExecutor openCypherQueryExecutor =
                new OpenCypherQueryExecutor(connection.getOpenCypherConnectionProperties());
        return openCypherQueryExecutor.executeGetColumns(getConnection().createStatement(), tableNamePattern);
    }
}
