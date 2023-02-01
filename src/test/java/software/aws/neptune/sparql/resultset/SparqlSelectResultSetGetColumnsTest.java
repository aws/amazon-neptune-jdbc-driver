/*
 * Copyright <2023> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.sparql.resultset;

import org.junit.jupiter.api.BeforeAll;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.sparql.SparqlConnection;
import software.aws.neptune.sparql.SparqlConnectionProperties;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SparqlSelectResultSetGetColumnsTest {
    protected static final Properties PROPERTIES = new Properties();
    private static final Map<String, Map<String, Map<String, Object>>> COLUMNS = new HashMap<>();
    private static java.sql.Statement statement;

    /**
     * Function to initialize java.sql.Statement for use in tests.
     *
     * @throws SQLException Thrown if initialization fails.
     */
    @BeforeAll
    public static void initialize() throws SQLException {
        PROPERTIES.put(SparqlConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // reverse default to None
        // Make up fake endpoint since we aren't actually connection.
        PROPERTIES.putIfAbsent(SparqlConnectionProperties.ENDPOINT_KEY,
                "http://localhost");
        PROPERTIES.putIfAbsent(SparqlConnectionProperties.PORT_KEY,
                123);
        final java.sql.Connection connection = new SparqlConnection(new SparqlConnectionProperties(PROPERTIES));
        statement = connection.createStatement();
    }
}
