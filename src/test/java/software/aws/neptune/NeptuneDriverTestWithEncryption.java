/*
 * Copyright <2022> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import java.sql.SQLException;

/**
 * Test for NeptuneDriver Object.
 */
public class NeptuneDriverTestWithEncryption extends NeptuneDriverTestBase {
    private static final boolean WITH_ENCRYPTION = true;

    /**
     * Function to get a random available port and initialize database before testing,
     * with encryption enabled.
     */
    @BeforeAll
    public static void initializeDatabase() {
        initializeDatabase(WITH_ENCRYPTION);
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
        super.testAcceptsUrl(WITH_ENCRYPTION);
    }

    @Test
    @Disabled
    void testConnect() throws SQLException {
        super.testConnect(WITH_ENCRYPTION);
    }

    @Test
    @Disabled
    void testDriverManagerGetConnection() throws SQLException {
        super.testDriverManagerGetConnection(WITH_ENCRYPTION);
    }

    @Test
    void testDriverManagerGetDriver() throws SQLException {
        super.testDriverManagerGetDriver(WITH_ENCRYPTION);
    }
}
