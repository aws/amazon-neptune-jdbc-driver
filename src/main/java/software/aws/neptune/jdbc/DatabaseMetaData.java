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

package software.aws.neptune.jdbc;

import org.slf4j.LoggerFactory;
import software.aws.neptune.common.EmptyResultSet;
import software.aws.neptune.jdbc.utilities.CastHelper;
import software.aws.neptune.jdbc.utilities.SqlError;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

/**
 * Abstract implementation of DatabaseMetaData for JDBC Driver.
 */
public abstract class DatabaseMetaData implements java.sql.DatabaseMetaData {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DatabaseMetaData.class);
    // TODO: Create class with abstract functions to get these (and other) of constants
    private static final int MAX_CATALOG_NAME_LENGTH = 60;
    private static final int MAX_TABLE_NAME_LENGTH = 60;
    private static final int MAX_STATEMENT_LENGTH = 65536;
    private final Connection connection;

    /**
     * DatabaseMetaData constructor.
     *
     * @param connection Connection Object.
     */
    public DatabaseMetaData(final java.sql.Connection connection) {
        this.connection = (Connection) connection;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return CastHelper.unwrap(iface, LOGGER, this);
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return CastHelper.isWrapperFor(iface, this);
    }

    @Override
    public java.sql.Connection getConnection() {
        return connection;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return java.sql.Connection.TRANSACTION_NONE;
    }

    @Override
    public int getDriverMajorVersion() {
        return Driver.DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return Driver.DRIVER_MINOR_VERSION;
    }

    @Override
    public String getDriverVersion() {
        return Driver.DRIVER_VERSION;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public int getSQLStateType() {
        return java.sql.DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    @Override
    public int getMaxCatalogNameLength() {
        return MAX_CATALOG_NAME_LENGTH;
    }

    @Override
    public int getMaxStatementLength() {
        return MAX_STATEMENT_LENGTH;
    }

    @Override
    public int getMaxTableNameLength() {
        return MAX_TABLE_NAME_LENGTH;
    }

    @Override
    public boolean supportsResultSetConcurrency(final int type, final int concurrency) {
        return (type == ResultSet.TYPE_FORWARD_ONLY) && (concurrency == ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public boolean supportsResultSetType(final int type) {
        return (ResultSet.TYPE_FORWARD_ONLY == type);
    }

    @Override
    public String getProcedureTerm() {
        LOGGER.debug("Procedures are not supported. Returning null.");
        return "";
    }

    @Override
    public String getSchemaTerm() {
        LOGGER.debug("Schemas are not supported. Returning an empty string.");
        return "";
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        LOGGER.debug("Binary is not a supported data type.");
        return 0;
    }

    @Override
    public ResultSet getCrossReference(final String parentCatalog, final String parentSchema, final String parentTable,
                                       final String foreignCatalog, final String foreignSchema,
                                       final String foreignTable)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public ResultSet getExportedKeys(final String catalog, final String schema, final String table)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public ResultSet getFunctionColumns(final String catalog, final String schemaNamePattern,
                                        final String tableNamePattern, final String columnNamePattern)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public ResultSet getFunctions(final String catalog, final String schemaPattern, final String functionNamePattern)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public ResultSet getProcedureColumns(final String catalog, final String schemaPattern,
                                         final String procedureNamePattern, final String columnNamePattern)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public ResultSet getPseudoColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
                                      final String columnNamePattern) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public ResultSet getSuperTables(final String catalog, final String schemaPattern, final String tableNamePattern)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public ResultSet getSuperTypes(final String catalog, final String schemaPattern, final String tableNamePattern)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public ResultSet getTablePrivileges(final String catalog, final String schemaPattern, final String tableNamePattern)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public ResultSet getUDTs(final String catalog, final String schemaPattern, final String typeNamePattern,
                             final int[] types) throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public ResultSet getVersionColumns(final String catalog, final String schema, final String table)
            throws SQLException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER);
    }

    @Override
    public int getMaxTablesInSelect() {
        return 1;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public boolean deletesAreDetected(final int type) {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    @Override
    public boolean insertsAreDetected(final int type) {
        return false;
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(final int fromType, final int toType) {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsResultSetHoldability(final int holdability) {
        return false;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(final int level) {
        return false;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsUnion() {
        return false;
    }

    @Override
    public boolean supportsUnionAll() {
        return false;
    }

    @Override
    public boolean updatesAreDetected(final int type) {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Override
    public int getMaxStatements() {
        return 0;
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
        return "'";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public ResultSet getProcedures(final String catalog, final String schemaPattern, final String procedureNamePattern)
            throws SQLException {
        return new EmptyResultSet(getConnection().createStatement());
    }

    @Override
    public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
        // No support for getSchemas other than empty result set so we can just invoke getSchema().
        return getSchemas();
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
    public ResultSet getClientInfoProperties() throws SQLException {
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
    public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern,
                               final String[] types)
            throws SQLException {
        // Only tableNamePattern is supported as an exact node label semicolon delimited String.
        LOGGER.info("Getting database tables.");
        return connection.getQueryExecutor().executeGetTables(getConnection().createStatement(), tableNamePattern);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        LOGGER.info("Getting database schemas.");
        return connection.getQueryExecutor().executeGetSchemas(getConnection().createStatement());
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        LOGGER.info("Getting database catalogs.");
        return connection.getQueryExecutor().executeGetCatalogs(getConnection().createStatement());
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        LOGGER.info("Getting database table types.");
        return connection.getQueryExecutor().executeGetTableTypes(getConnection().createStatement());
    }

    @Override
    public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
                                final String columnNamePattern)
            throws SQLException {
        if (catalog != null) {
            LOGGER.warn("Catalog in getColumns is not supported, ignoring {}.", catalog);
        }
        if (columnNamePattern != null) {
            LOGGER.warn("ColumnNamePattern in getColumns is not supported, ignoring {}.", columnNamePattern);
        }
        if (schemaPattern != null) {
            LOGGER.warn("SchemaPattern in getColumns is not supported, ignoring {}.", schemaPattern);
        }
        try {
            LOGGER.info("Getting database columns.");
            final ResultSet resultSet = connection.getQueryExecutor()
                    .executeGetColumns(getConnection().createStatement(), tableNamePattern);
            LOGGER.info("Database columns retrieved.");
            return resultSet;
        } catch (final Exception e) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            LOGGER.error("Encountered exception", e);
            throw new SQLException(e);
        }
    }
}
