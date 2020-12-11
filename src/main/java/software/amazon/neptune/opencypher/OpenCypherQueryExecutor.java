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
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import java.sql.SQLException;

/**
 * OpenCypher implementation of QueryExecution.
 */
public class OpenCypherQueryExecutor {
    private final Driver driver;
    private static final int MAX_FETCH_SIZE = Integer.MAX_VALUE;
    private final int fetchSize = -1;
    private boolean isConfigChange = false;
    private boolean isSessionConfigChange = false;
    private int queryTimeout = -1;
    private Config config;
    private SessionConfig sessionConfig;

    /**
     * OpenCypherQueryExecutor constructor.
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


    protected void cancelQuery() throws SQLException {
        // TODO: Cancel logic.
    }

    protected int getMaxFetchSize() throws SQLException {
        return MAX_FETCH_SIZE;
    }

    /**
     * Function to execute query.
     * @param sql Query to execute.
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet object returned from query execution.
     */
    public java.sql.ResultSet executeQuery(final String sql, final java.sql.Statement statement) {
        final Session session = driver.session(sessionConfig);
        return new OpenCypherResultSet(statement, session.run(sql), session);
        // TODO: Throw exception?
    }

    /**
     * Get query execution timeout in seconds.
     * @return Query execution timeout in seconds.
     */
    public int getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Set query execution timeout to the timeout in seconds.
     * @param seconds Time in seconds to set query timeout to.
     */
    public void setQueryTimeout(final int seconds) {
        isConfigChange = true;
        isSessionConfigChange = true;
        queryTimeout = seconds;
    }
}
