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

package software.aws.neptune.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.aws.neptune.jdbc.helpers.HelperFunctions;
import software.aws.neptune.jdbc.mock.MockDataSource;
import software.aws.neptune.jdbc.mock.MockStatement;
import software.aws.neptune.jdbc.utilities.SqlError;

/**
 * Test for abstract DataSource Object.
 */
public class DataSourceTest {

    private javax.sql.DataSource dataSource;

    @BeforeEach
    void initialize() {
        dataSource = new MockDataSource();
    }

    @Test
    void testUnwrap() {
        HelperFunctions.expectFunctionDoesntThrow(() -> dataSource.isWrapperFor(MockDataSource.class), true);
        HelperFunctions.expectFunctionDoesntThrow(() -> dataSource.isWrapperFor(MockStatement.class), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> dataSource.isWrapperFor(null), false);
        HelperFunctions.expectFunctionDoesntThrow(() -> dataSource.unwrap(MockDataSource.class), dataSource);
        HelperFunctions.expectFunctionThrows(() -> dataSource.unwrap(MockStatement.class));
    }

    @Test
    void testLoggers() {
        HelperFunctions.expectFunctionDoesntThrow(() -> dataSource.getLogWriter(), null);
        HelperFunctions.expectFunctionDoesntThrow(() -> dataSource.setLogWriter(null));
        HelperFunctions.expectFunctionDoesntThrow(() -> dataSource.getLogWriter(), null);
        HelperFunctions.expectFunctionThrows(SqlError.FEATURE_NOT_SUPPORTED, () -> dataSource.getParentLogger());
    }
}
