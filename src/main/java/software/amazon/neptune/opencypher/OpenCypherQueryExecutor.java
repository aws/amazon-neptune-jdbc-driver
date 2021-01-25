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

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;

import java.sql.SQLException;
import java.util.List;

/**
 * OpenCypher implementation of QueryExecution.
 */
public class OpenCypherQueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherQueryExecutor.class);
    private final Driver driver;
    private static final int MAX_FETCH_SIZE = Integer.MAX_VALUE;
    private final int fetchSize = -1;
    private boolean isConfigChange = false;
    private boolean isSessionConfigChange = false;
    private int queryTimeout = -1;
    private Config config;
    private SessionConfig sessionConfig;
    private Session session;
    private final Object lock = new Object();
    private boolean queryExecuted = false;
    private boolean queryCancelled = false;

    /**
     * OpenCypherQueryExecutor constructor.
     *
     * @param properties properties to use for query executon.
     */
    OpenCypherQueryExecutor(final OpenCypherConnectionProperties properties) {
        final String endpoint = properties.getEndpoint();
        // TODO: Implement authentication.
        // final String user = properties.getUser();
        // final String password = properties.getPassword();
        // AuthTokens.basic(this.user, this.password), this.config);
        this.config = Config.builder().build();
        this.sessionConfig = SessionConfig.builder().build();
        this.driver = GraphDatabase.driver(endpoint, this.config);
    }

    Config getConfig() {
        if (isConfigChange) {
            final Config.ConfigBuilder builder = Config.builder();
            if (fetchSize != -1) {
                builder.withFetchSize(fetchSize);
            }
            // TODO: More Configs.
            config = builder.build();
        }
        return config;
    }

    SessionConfig getSessionConfig() {
        if (isSessionConfigChange) {
            final SessionConfig.Builder builder = SessionConfig.builder();
            if (fetchSize != -1) {
                // TODO: This is duplicated with Config, look into this.
                builder.withFetchSize(fetchSize);
            }
            // TODO: More SessionConfigs.
            sessionConfig = builder.build();
        }
        return sessionConfig;
    }

    protected int getMaxFetchSize() throws SQLException {
        return MAX_FETCH_SIZE;
    }

    /**
     * Function to execute query.
     *
     * @param sql       Query to execute.
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    public java.sql.ResultSet executeQuery(final String sql, final java.sql.Statement statement) throws SQLException {
        synchronized (lock) {
            queryCancelled = false;
            queryExecuted = false;
        }

        try {
            session = driver.session(sessionConfig);
            final Result result = session.run(sql);
            final List<Record> rows = result.list();
            final List<String> columns = result.keys();
            synchronized (lock) {
                if (queryCancelled) {
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_CANCELED);
                }
                queryExecuted = true;
            }
            return new OpenCypherResultSet(
                    statement, session, result, rows, columns);

        } catch (RuntimeException e) {
            synchronized (lock) {
                if (queryCancelled) {
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_CANCELED);
                } else {
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_FAILED, e.getMessage());
                }
            }
        }
    }

    /**
     * Function to cancel running query.
     * This has to be run in the different thread from the one running the query.
     * @throws SQLException if query cancellation fails.
     */
    protected void cancelQuery() throws SQLException {
        LOGGER.info("Cancel query invoked.");
        synchronized (lock) {
            if (session == null) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_NOT_STARTED);
            }

            if (queryCancelled) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_CANCELED);
            }

            if (!queryExecuted) {
                //noinspection deprecation
                session.reset();
                LOGGER.debug("Cancel query succeeded.");
                queryCancelled = true;
            } else {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_CANNOT_BE_CANCELLED);
            }
        }
    }

    /**
     * Get query execution timeout in seconds.
     *
     * @return Query execution timeout in seconds.
     */
    public int getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Set query execution timeout to the timeout in seconds.
     *
     * @param seconds Time in seconds to set query timeout to.
     */
    public void setQueryTimeout(final int seconds) {
        isConfigChange = true;
        isSessionConfigChange = true;
        queryTimeout = seconds;
    }
}
