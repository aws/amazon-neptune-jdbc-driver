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
import software.aws.neptune.opencypher.OpenCypherConnection;
import software.aws.neptune.opencypher.OpenCypherConnectionProperties;
import software.aws.performance.implementations.PerformanceTestConstants;
import java.sql.Statement;
import java.util.Properties;

public class OpenCypherJDBCExecutor extends JDBCExecutor {
    private final java.sql.Connection connection;

    /**
     * Constructor for OpenCypherJDBCExecutor.
     */
    @SneakyThrows
    public OpenCypherJDBCExecutor() {
        final Properties properties = new Properties();
        properties.put(OpenCypherConnectionProperties.ENDPOINT_KEY,
                String.format("bolt://%s:%d", PerformanceTestConstants.ENDPOINT, PerformanceTestConstants.PORT));
        properties.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, PerformanceTestConstants.AUTH_SCHEME);
        properties.put(OpenCypherConnectionProperties.SERVICE_REGION_KEY, PerformanceTestConstants.REGION);
        connection = new OpenCypherConnection(new OpenCypherConnectionProperties(properties));

    }

    @Override
    @SneakyThrows
    Statement getNewStatement() {
        return connection.createStatement();
    }
}
