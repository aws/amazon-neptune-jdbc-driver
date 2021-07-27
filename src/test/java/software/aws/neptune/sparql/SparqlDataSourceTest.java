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

package software.aws.neptune.sparql;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.aws.jdbc.helpers.HelperFunctions;
import software.aws.jdbc.utilities.AuthScheme;
import software.aws.jdbc.utilities.SqlError;
import software.aws.neptune.sparql.mock.SparqlMockServer;
import java.sql.SQLException;

public class SparqlDataSourceTest {
    private static final String HOSTNAME = "http://localhost";
    private static final String VALID_ENDPOINT = String.format("sparql://%s:%d", HOSTNAME, SparqlMockServer.port());
    private SparqlDataSource dataSource;

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeMockServer() {
        SparqlMockServer.ctlBeforeClass();
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownMockServer() {
        SparqlMockServer.ctlAfterClass();
    }

    @BeforeEach
    void initialize() throws SQLException {
        dataSource = new SparqlDataSource();
    }

    @Test
    void testGetConnectionSuccess() throws SQLException {
        dataSource.setEndpoint(VALID_ENDPOINT);
        Assertions.assertTrue(dataSource.getConnection() instanceof SparqlConnection);
        Assertions.assertTrue(dataSource.getPooledConnection() instanceof SparqlPooledConnection);
    }

    @Test
    void testGetConnectionFailure() throws SQLException {
        HelperFunctions
                .expectFunctionThrows(SqlError.FEATURE_NOT_SUPPORTED, () -> dataSource.getConnection("name", "psw"));
        HelperFunctions.expectFunctionThrows(SqlError.FEATURE_NOT_SUPPORTED,
                () -> dataSource.getPooledConnection("name", "psw"));
    }

    @Test
    void testSupportedProperties() throws SQLException {
        Assertions.assertDoesNotThrow(() -> dataSource.setEndpoint(VALID_ENDPOINT));
        Assertions.assertEquals(VALID_ENDPOINT, dataSource.getEndpoint());

        Assertions.assertDoesNotThrow(() -> dataSource.setApplicationName("appName"));
        Assertions.assertEquals("appName", dataSource.getApplicationName());

        Assertions.assertDoesNotThrow(() -> dataSource.setAuthScheme(AuthScheme.None));
        Assertions.assertEquals(AuthScheme.None, dataSource.getAuthScheme());

        Assertions.assertDoesNotThrow(() -> dataSource.setConnectionRetryCount(5));
        Assertions.assertEquals(5, dataSource.getConnectionRetryCount());

        Assertions.assertDoesNotThrow(() -> dataSource.setConnectionTimeoutMillis(1000));
        Assertions.assertEquals(1000, dataSource.getConnectionTimeoutMillis());

        Assertions.assertDoesNotThrow(() -> dataSource.setLoginTimeout(50));
        Assertions.assertEquals(50, dataSource.getLoginTimeout());
    }
}
