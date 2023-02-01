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

package software.aws.neptune.opencypher;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.neptune.NeptuneDriverTestWithEncryption;
import software.aws.neptune.jdbc.helpers.HelperFunctions;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.opencypher.mock.MockOpenCypherDatabase;
import java.sql.SQLException;

class OpenCypherDataSourceTest {
    private static MockOpenCypherDatabase database;
    private static String validEndpoint;

    private OpenCypherDataSource dataSource;

    /**
     * Function to get a random available port and initialize database before testing.
     */
    @BeforeAll
    public static void initializeDatabase() {
        database = MockOpenCypherDatabase.builder("localhost", NeptuneDriverTestWithEncryption.class.getName()).build();
        validEndpoint = String.format("bolt://%s:%d", "localhost", database.getPort());
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        database.shutdown();
    }

    @BeforeEach
    void initialize() throws SQLException {
        dataSource = new OpenCypherDataSource();
    }

    @Test
    @Disabled
    void testGetConnectionSuccess() throws SQLException {
        dataSource.setEndpoint(validEndpoint);
        Assertions.assertTrue(dataSource.getConnection() instanceof OpenCypherConnection);
        Assertions.assertTrue(dataSource.getPooledConnection() instanceof OpenCypherPooledConnection);
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
        Assertions.assertDoesNotThrow(() -> dataSource.setEndpoint(validEndpoint));
        Assertions.assertEquals(validEndpoint, dataSource.getEndpoint());

        Assertions.assertDoesNotThrow(() -> dataSource.setApplicationName("appName"));
        Assertions.assertEquals("appName", dataSource.getApplicationName());

        Assertions.assertDoesNotThrow(() -> dataSource.setAuthScheme(AuthScheme.None));
        Assertions.assertEquals(AuthScheme.None, dataSource.getAuthScheme());

        Assertions.assertDoesNotThrow(() -> dataSource.setAwsCredentialsProviderClass("aws"));
        Assertions.assertEquals("aws", dataSource.getAwsCredentialsProviderClass());

        Assertions.assertDoesNotThrow(() -> dataSource.setCustomCredentialsFilePath("path"));
        Assertions.assertEquals("path", dataSource.getCustomCredentialsFilePath());

        Assertions.assertDoesNotThrow(() -> dataSource.setConnectionRetryCount(5));
        Assertions.assertEquals(5, dataSource.getConnectionRetryCount());

        Assertions.assertDoesNotThrow(() -> dataSource.setConnectionTimeoutMillis(1000));
        Assertions.assertEquals(1000, dataSource.getConnectionTimeoutMillis());

        Assertions.assertDoesNotThrow(() -> dataSource.setLoginTimeout(50));
        Assertions.assertEquals(50, dataSource.getLoginTimeout());

        Assertions.assertDoesNotThrow(() -> dataSource.setUseEncryption(true));
        Assertions.assertEquals(true, dataSource.getUseEncryption());
    }
}
