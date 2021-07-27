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

package software.aws.jdbc.utilities;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.Properties;

public abstract class QueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    private final Object lock = new Object();
    @Setter
    @Getter
    private int queryTimeout = -1;
    @Setter
    @Getter
    private int fetchSize = Integer.MAX_VALUE;
    private QueryState queryState = QueryState.NOT_STARTED;

    protected static boolean propertiesEqual(
            final ConnectionProperties connectionProperties1,
            final ConnectionProperties connectionProperties2) {
        final Properties properties1 = connectionProperties1.getProperties();
        final Properties properties2 = connectionProperties2.getProperties();
        if (!properties1.keySet().equals(properties2.keySet())) {
            return false;
        }
        for (final Object key : properties1.keySet()) {
            if (!properties1.get(key).equals(properties2.get(key))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Function to get max fetch size for driver.
     *
     * @return Max fetch size of driver.
     */
    public abstract int getMaxFetchSize();

    /**
     * Verify that connection to database is functional.
     *
     * @param timeout Time in seconds to wait for the database operation used to validate the connection to complete.
     * @return true if the connection is valid, otherwise false.
     */
    public abstract boolean isValid(final int timeout);

    /**
     * Function to execute query.
     *
     * @param sql       Query to execute.
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    public abstract java.sql.ResultSet executeQuery(final String sql, final java.sql.Statement statement) throws
            SQLException;

    /**
     * Function to get tables.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    public abstract java.sql.ResultSet executeGetTables(final java.sql.Statement statement, final String tableName)
            throws SQLException;

    /**
     * Function to get schema.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing schemas.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    public abstract java.sql.ResultSet executeGetSchemas(final java.sql.Statement statement)
            throws SQLException;

    /**
     * Function to get catalogs.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing catalogs.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    public abstract java.sql.ResultSet executeGetCatalogs(final java.sql.Statement statement)
            throws SQLException;

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing table types.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    public abstract java.sql.ResultSet executeGetTableTypes(final java.sql.Statement statement)
            throws SQLException;

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @param nodes     String containing nodes to get schema for.
     * @return java.sql.ResultSet Object containing columns.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    public abstract java.sql.ResultSet executeGetColumns(final java.sql.Statement statement, final String nodes)
            throws SQLException;

    /**
     * This function is supposed to run the queries and construct the target ResultSet using reflection.
     *
     * @param constructor Target ResultSet type.
     * @param statement   Statement which is issuing query.
     * @param query       Query to execute.
     * @return Target ResultSet Object.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    protected <T> java.sql.ResultSet runCancellableQuery(final Constructor<?> constructor,
                                                         final java.sql.Statement statement,
                                                         final String query) throws SQLException {
        synchronized (lock) {
            if (queryState.equals(QueryState.IN_PROGRESS)) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_IN_PROGRESS);
            }
            queryState = QueryState.IN_PROGRESS;
        }

        try {
            final T intermediateResult = runQuery(query);
            synchronized (lock) {
                if (queryState.equals(QueryState.CANCELLED)) {
                    resetQueryState();
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_CANCELED);
                }
                resetQueryState();
            }
            return (java.sql.ResultSet) constructor.newInstance(statement, intermediateResult);
        } catch (final SQLException e) {
          throw e;
        } catch (final Exception e) {
            synchronized (lock) {
                if (queryState.equals(QueryState.CANCELLED)) {
                    resetQueryState();
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_CANCELED);
                } else {
                    resetQueryState();
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_FAILED, e);
                }
            }
        }
    }

    private void resetQueryState() {
        queryState = QueryState.NOT_STARTED;
    }

    protected abstract <T> T runQuery(final String query) throws SQLException;

    /**
     * Function to cancel running query.
     * This has to be run in the different thread from the one running the query.
     *
     * @throws SQLException if query cancellation fails.
     */
    public void cancelQuery() throws SQLException {
        synchronized (lock) {
            if (queryState.equals(QueryState.NOT_STARTED)) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_NOT_STARTED_OR_COMPLETE);
            } else if (queryState.equals(QueryState.CANCELLED)) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_CANCELED);
            }

            performCancel();
            queryState = QueryState.CANCELLED;
            LOGGER.debug("Cancel query succeeded.");
        }
    }

    protected abstract void performCancel() throws SQLException;

    enum QueryState {
        NOT_STARTED,
        IN_PROGRESS,
        CANCELLED
    }
}
