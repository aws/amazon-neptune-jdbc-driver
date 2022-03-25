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

package software.aws.neptune.jdbc.mock;

import software.aws.neptune.jdbc.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Mock implementation for Driver object so it can be instantiated and tested.
 */
public class MockDriver extends Driver implements java.sql.Driver {
    @Override
    public Connection connect(final String url, final Properties info) throws SQLException {
        return null;
    }

    @Override
    public boolean acceptsURL(final String url) throws SQLException {
        return false;
    }
}
