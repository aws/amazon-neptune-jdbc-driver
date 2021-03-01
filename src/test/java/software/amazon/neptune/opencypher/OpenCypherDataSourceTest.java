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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.neptune.NeptuneDriverTestWithEncryption;
import software.amazon.neptune.opencypher.mock.MockOpenCypherDatabase;

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
    void testGetConnectionSuccess() throws SQLException {
        dataSource.setEndpoint(validEndpoint);
        Assertions.assertTrue(dataSource.getConnection() instanceof OpenCypherConnection);
    }
}
