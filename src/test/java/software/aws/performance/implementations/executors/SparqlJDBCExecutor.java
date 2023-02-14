/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.performance.implementations.executors;

import lombok.SneakyThrows;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import software.aws.neptune.sparql.SparqlConnection;
import software.aws.neptune.sparql.SparqlConnectionProperties;
import software.aws.performance.implementations.PerformanceTestConstants;
import java.sql.Statement;
import java.util.Properties;

public class SparqlJDBCExecutor extends JDBCExecutor {
    private final java.sql.Connection connection;

    /**
     * Constructor for SparqlJDBCExecutor.
     */
    @SneakyThrows
    public SparqlJDBCExecutor() {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, PerformanceTestConstants.AUTH_SCHEME);
        properties.put(SparqlConnectionProperties.ENDPOINT_KEY, PerformanceTestConstants.SPARQL_ENDPOINT);
        properties.put(SparqlConnectionProperties.PORT_KEY, PerformanceTestConstants.PORT);
        properties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, PerformanceTestConstants.SPARQL_QUERY);
        connection = new SparqlConnection(new SparqlConnectionProperties(properties));
    }

    @Override
    @SneakyThrows
    Statement getNewStatement() {
        return connection.createStatement();
    }
}
