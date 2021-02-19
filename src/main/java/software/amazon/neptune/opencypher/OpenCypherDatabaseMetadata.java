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
import software.amazon.jdbc.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * OpenCypher implementation of DatabaseMetaData.
 */
public class OpenCypherDatabaseMetadata extends DatabaseMetaData implements java.sql.DatabaseMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherDatabaseMetadata.class);

    /**
     * OpenCypherDatabaseMetadata constructor, initializes super class.
     *
     * @param connection Connection Object.
     */
    OpenCypherDatabaseMetadata(final java.sql.Connection connection) {
        super(connection);
    }

    // TODO: Go through and implement these functions
    @Override
    public String getURL() throws SQLException {
        return null;
    }

    @Override
    public String getUserName() throws SQLException {
        return null;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return null;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return null;
    }

    @Override
    public String getDriverName() throws SQLException {
        return null;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return null;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return null;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return null;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return null;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return null;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public ResultSet getProcedures(final String catalog, final String schemaPattern, final String procedureNamePattern)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern,
                               final String[] types)
            throws SQLException {
        // Only tableNamePattern is supported as an exact node label semicolon delimited String.
        LOGGER.info("Getting database tables.");
        final OpenCypherQueryExecutor openCypherQueryExecutor = new OpenCypherQueryExecutor(
                new OpenCypherConnectionProperties(getConnection().getClientInfo()));
        return openCypherQueryExecutor.executeGetTables(getConnection().createStatement(), tableNamePattern);
    }

    @Override
    public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
        // No support for getSchemas other than empty result set so we can just invoke getSchema().
        return getSchemas();
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        LOGGER.info("Getting database schemas.");
        final OpenCypherQueryExecutor openCypherQueryExecutor = new OpenCypherQueryExecutor(
                new OpenCypherConnectionProperties(getConnection().getClientInfo()));
        return openCypherQueryExecutor.executeGetSchemas(getConnection().createStatement());
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        LOGGER.info("Getting database catalogs.");
        final OpenCypherQueryExecutor openCypherQueryExecutor = new OpenCypherQueryExecutor(
                new OpenCypherConnectionProperties(getConnection().getClientInfo()));
        return openCypherQueryExecutor.executeGetCatalogs(getConnection().createStatement());
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        LOGGER.info("Getting database table types.");
        final OpenCypherQueryExecutor openCypherQueryExecutor = new OpenCypherQueryExecutor(
                new OpenCypherConnectionProperties(getConnection().getClientInfo()));
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
        final OpenCypherQueryExecutor openCypherQueryExecutor = new OpenCypherQueryExecutor(
                new OpenCypherConnectionProperties(getConnection().getClientInfo()));
        return openCypherQueryExecutor.executeGetColumns(getConnection().createStatement(), tableNamePattern);
    }

    @Override
    public ResultSet getColumnPrivileges(final String catalog, final String schema, final String table,
                                         final String columnNamePattern)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(final String catalog, final String schema, final String table,
                                          final int scope, final boolean nullable)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getImportedKeys(final String catalog, final String schema, final String table)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getIndexInfo(final String catalog, final String schema, final String table, final boolean unique,
                                  final boolean approximate)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(final String catalog, final String schemaPattern, final String typeNamePattern,
                                   final String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }
}
