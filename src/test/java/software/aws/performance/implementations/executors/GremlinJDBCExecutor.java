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
import software.aws.neptune.gremlin.GremlinConnection;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.performance.implementations.PerformanceTestConstants;
import java.sql.Statement;
import java.util.Properties;

import static software.aws.neptune.gremlin.GremlinConnectionProperties.CONTACT_POINT_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.PORT_KEY;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.AUTH_SCHEME_KEY;

public class GremlinJDBCExecutor extends JDBCExecutor {
    private final java.sql.Connection connection;

    /**
     * Constructor for GremlinJDBCExecutor.
     */
    @SneakyThrows
    public GremlinJDBCExecutor() {
        final Properties properties = new Properties();
        properties.put(CONTACT_POINT_KEY, PerformanceTestConstants.ENDPOINT);
        properties.put(PORT_KEY, PerformanceTestConstants.PORT);
        properties.put(AUTH_SCHEME_KEY, PerformanceTestConstants.AUTH_SCHEME);
        connection = new GremlinConnection(new GremlinConnectionProperties(properties));

    }

    @Override
    @SneakyThrows
    Statement getNewStatement() {
        return connection.createStatement();
    }
}
