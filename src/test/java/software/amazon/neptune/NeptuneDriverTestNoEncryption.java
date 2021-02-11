/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.neptune;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class NeptuneDriverTestNoEncryption extends NeptuneDriverTestBase {
    private static final boolean NO_ENCRYPTION = false;

    /**
     * Function to get a random available port and initialize database before testing,
     * with encryption disabled.
     */
    @BeforeAll
    public static void initializeDatabase() {
        initializeDatabase(NO_ENCRYPTION);
    }

    /**
     * Function to get a shutdown database after testing.
     */
    @AfterAll
    public static void shutdownDatabase() {
        shutdownTheDatabase();
    }

    @BeforeEach
    void initialize() {
        super.initialize();
    }

    @Test
    void testAcceptsUrl() throws SQLException {
        super.testAcceptsUrl(NO_ENCRYPTION);
    }

    @Test
    void testConnect() throws SQLException {
        super.testConnect(NO_ENCRYPTION);
    }

    @Test
    void testDriverManagerGetConnection() throws SQLException {
        super.testDriverManagerGetConnection(NO_ENCRYPTION);
    }

    @Test
    void testDriverManagerGetDriver() throws SQLException {
        super.testDriverManagerGetDriver(NO_ENCRYPTION);
    }
}
