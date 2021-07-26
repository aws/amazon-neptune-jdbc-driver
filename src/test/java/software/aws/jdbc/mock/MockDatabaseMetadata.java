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

package software.aws.jdbc.mock;

import software.aws.jdbc.DatabaseMetaData;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Mock implementation for DatabaseMetadata object so it can be instantiated and tested.
 */
public class MockDatabaseMetadata extends DatabaseMetaData implements java.sql.DatabaseMetaData {

    /**
     * MockDatabaseMetadata constructor.
     *
     * @param connection Connection Object.
     */
    public MockDatabaseMetadata(final Connection connection) {
        super(connection);
    }

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
        return null;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
                                final String columnNamePattern)
            throws SQLException {
        return null;
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
    public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }
}
