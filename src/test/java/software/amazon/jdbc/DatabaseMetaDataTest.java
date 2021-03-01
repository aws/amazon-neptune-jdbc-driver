/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.helpers.HelperFunctions;
import software.amazon.jdbc.mock.MockConnection;
import software.amazon.jdbc.mock.MockDatabaseMetadata;
import software.amazon.jdbc.mock.MockStatement;
import software.amazon.neptune.opencypher.OpenCypherConnectionProperties;

import java.sql.RowIdLifetime;
import java.sql.SQLException;

/**
 * Test for abstract DatabaseMetaData Object.
 */
public class DatabaseMetaDataTest {
    private java.sql.DatabaseMetaData databaseMetaData;
    private java.sql.Connection connection;

    @BeforeEach
    void initialize() throws SQLException {
        connection = new MockConnection(new OpenCypherConnectionProperties());
        databaseMetaData = new MockDatabaseMetadata(connection);
    }

    @Test
    void testSupport() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsANSI92FullSQL(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsANSI92IntermediateSQL(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsAlterTableWithAddColumn(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsAlterTableWithDropColumn(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsBatchUpdates(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsCatalogsInDataManipulation(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsCatalogsInIndexDefinitions(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsCatalogsInPrivilegeDefinitions(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsCatalogsInProcedureCalls(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsCatalogsInTableDefinitions(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsColumnAliasing(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsConvert(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsConvert(0, 0), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsCoreSQLGrammar(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsCorrelatedSubqueries(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsDataDefinitionAndDataManipulationTransactions(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsDataManipulationTransactionsOnly(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsDifferentTableCorrelationNames(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsExpressionsInOrderBy(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsExtendedSQLGrammar(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsFullOuterJoins(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsGetGeneratedKeys(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsGroupBy(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsGroupByBeyondSelect(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsGroupByUnrelated(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsIntegrityEnhancementFacility(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsLikeEscapeClause(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsLimitedOuterJoins(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsMinimumSQLGrammar(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsMixedCaseIdentifiers(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsMixedCaseQuotedIdentifiers(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsMultipleOpenResults(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsMultipleResultSets(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsMultipleTransactions(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsNamedParameters(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsNonNullableColumns(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsOpenCursorsAcrossCommit(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsOpenCursorsAcrossRollback(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsOpenStatementsAcrossCommit(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsOpenStatementsAcrossRollback(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsOrderByUnrelated(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsOuterJoins(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsPositionedDelete(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsPositionedUpdate(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsResultSetHoldability(0), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsSavepoints(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsSchemasInDataManipulation(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsSchemasInIndexDefinitions(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsSchemasInPrivilegeDefinitions(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsSchemasInProcedureCalls(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsSchemasInTableDefinitions(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsSelectForUpdate(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsStatementPooling(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsStoredFunctionsUsingCallSyntax(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsStoredProcedures(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsSubqueriesInComparisons(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsSubqueriesInExists(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsSubqueriesInIns(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsSubqueriesInQuantifieds(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsTableCorrelationNames(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsTransactionIsolationLevel(0), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsTransactions(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsUnion(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsUnionAll(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsResultSetConcurrency(
                java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsResultSetConcurrency(
                java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_UPDATABLE), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsResultSetConcurrency(
                java.sql.ResultSet.TYPE_SCROLL_SENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsResultSetType(
                java.sql.ResultSet.TYPE_FORWARD_ONLY), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsResultSetType(
                java.sql.ResultSet.TYPE_SCROLL_SENSITIVE), false);

    }

    @Test
    void testMaxValues() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxCharLiteralLength(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxColumnNameLength(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxColumnsInGroupBy(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxColumnsInIndex(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxColumnsInOrderBy(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxColumnsInSelect(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxColumnsInTable(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxConnections(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxCursorNameLength(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxIndexLength(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxProcedureNameLength(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxSchemaNameLength(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxStatements(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxBinaryLiteralLength(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxTablesInSelect(), 1);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxUserNameLength(), 0);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxCatalogNameLength(), 60);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxTableNameLength(), 60);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getMaxStatementLength(), 65536);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getDefaultTransactionIsolation(),
                java.sql.Connection.TRANSACTION_NONE);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getResultSetHoldability(),
                java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getSQLStateType(),
                java.sql.DatabaseMetaData.sqlStateSQL);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getProcedureTerm(), "");
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getSchemaTerm(), "");
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getIdentifierQuoteString(), "\"");
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getRowIdLifetime(), RowIdLifetime.ROWID_UNSUPPORTED);

        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.getCrossReference("", "", "", "", "", ""));
        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.getExportedKeys("", "", ""));
        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.getFunctionColumns("", "", "", ""));
        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.getFunctions("", "", ""));
        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.getProcedureColumns("", "", "", ""));
        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.getPseudoColumns("", "", "", ""));
        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.getTablePrivileges("", "", ""));
        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.getUDTs("", "", "", new int[]{}));
        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.getVersionColumns("", "", ""));
        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.getSuperTables("", "", ""));
        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.getSuperTypes("", "", ""));
    }

    @Test
    void testConnection() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getConnection(), connection);
    }

    @Test
    void testWrap() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.isWrapperFor(MockDatabaseMetadata.class), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.isWrapperFor(MockStatement.class), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.isWrapperFor(null), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.unwrap(MockDatabaseMetadata.class), databaseMetaData);
        HelperFunctions.expectFunctionThrows(() -> databaseMetaData.unwrap(MockStatement.class));
    }

    @Test
    void testDriverVersion() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getDriverMajorVersion(), Driver.DRIVER_MAJOR_VERSION);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getDriverMinorVersion(), Driver.DRIVER_MINOR_VERSION);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.getDriverVersion(), Driver.DRIVER_VERSION);
    }

    @Test
    void testUpdates() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.updatesAreDetected(1), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.usesLocalFilePerTable(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.usesLocalFiles(), false);
    }

    @Test
    void testAll() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.allProceduresAreCallable(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.allTablesAreSelectable(), true);
    }

    @Test
    void testDataDefinition() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.dataDefinitionCausesTransactionCommit(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.dataDefinitionIgnoredInTransactions(), false);
    }

    @Test
    void testMisc() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.autoCommitFailureClosesAllResultSets(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.deletesAreDetected(1), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.insertsAreDetected(1), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.locatorsUpdateCopy(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.generatedKeyAlwaysReturned(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.doesMaxRowSizeIncludeBlobs(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.isCatalogAtStart(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.isReadOnly(), true);
    }

    @Test
    void testNull() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.nullPlusNonNullIsNull(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.nullsAreSortedAtEnd(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.nullsAreSortedAtStart(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.nullsAreSortedHigh(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.nullsAreSortedLow(), false);
    }

    @Test
    void testOthers() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.othersDeletesAreVisible(1), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.othersInsertsAreVisible(1), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.othersUpdatesAreVisible(1), false);
    }

    @Test
    void testOwn() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.ownDeletesAreVisible(1), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.ownInsertsAreVisible(1), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.ownUpdatesAreVisible(1), false);

    }

    @Test
    void testStores() {
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.storesLowerCaseIdentifiers(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.storesLowerCaseQuotedIdentifiers(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.storesUpperCaseIdentifiers(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.storesUpperCaseQuotedIdentifiers(), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.storesMixedCaseIdentifiers(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.storesMixedCaseQuotedIdentifiers(), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> databaseMetaData.supportsANSI92EntryLevelSQL(), true);
    }
}
