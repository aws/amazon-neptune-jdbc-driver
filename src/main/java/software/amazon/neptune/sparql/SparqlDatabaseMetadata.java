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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.DatabaseMetaData;
import software.amazon.neptune.common.EmptyResultSet;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SparqlDatabaseMetadata extends DatabaseMetaData implements java.sql.DatabaseMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlDatabaseMetadata.class);
    private final SparqlConnection connection;

    /**
     * DatabaseMetaData constructor.
     *
     * @param connection Connection Object.
     */
    public SparqlDatabaseMetadata(final Connection connection) {
        super(connection);
        this.connection = (SparqlConnection) connection;
    }

    @Override
    public String getURL() throws SQLException {
        return "";
    }

    @Override
    public String getUserName() throws SQLException {
        return "";
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        // TODO: Is there a way to get this?
        return "Neptune";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        // TODO: Is there a way to get this?
        return "1.0";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "neptune:sparql";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "graph";
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return "";
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    //TODO: fill functions
    public ResultSet getProcedures(final String catalog, final String schemaPattern, final String procedureNamePattern) throws SQLException {
        return null;
    }

    @Override
    //TODO: fill functions
    public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern, final String[] types) throws SQLException {
        return null;
    }

    @Override
    //TODO: fill functions
    public ResultSet getSchemas() throws SQLException {
        return null;
    }

    @Override
    //TODO: fill functions
    public ResultSet getCatalogs() throws SQLException {
        return null;
    }

    @Override
    //TODO: fill functions
    public ResultSet getTableTypes() throws SQLException {
        return null;
    }

    @Override
    //TODO: fill functions
    public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern, final String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getColumnPrivileges(final String catalog, final String schema, final String table,
                                         final String columnNamePattern)
            throws SQLException {
        return new EmptyResultSet(getConnection().createStatement());
    }

    @Override
    public ResultSet getBestRowIdentifier(final String catalog, final String schema, final String table,
                                          final int scope, final boolean nullable)
            throws SQLException {
        return new EmptyResultSet(getConnection().createStatement());
    }

    @Override
    public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
        return new EmptyResultSet(getConnection().createStatement());
    }

    @Override
    public ResultSet getImportedKeys(final String catalog, final String schema, final String table)
            throws SQLException {
        return new EmptyResultSet(getConnection().createStatement());
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return new EmptyResultSet(getConnection().createStatement());
    }

    @Override
    public ResultSet getIndexInfo(final String catalog, final String schema, final String table, final boolean unique,
                                  final boolean approximate)
            throws SQLException {
        return new EmptyResultSet(getConnection().createStatement());
    }

    @Override
    public ResultSet getAttributes(final String catalog, final String schemaPattern, final String typeNamePattern,
                                   final String attributeNamePattern) throws SQLException {
        return new EmptyResultSet(getConnection().createStatement());
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
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 2;
    }

    @Override
    public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return new EmptyResultSet(getConnection().createStatement());
    }
}
