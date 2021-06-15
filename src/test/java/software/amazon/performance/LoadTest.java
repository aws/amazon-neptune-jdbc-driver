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

package software.amazon.performance;

import software.amazon.performance.config.LoadTestInfo;
import software.amazon.performance.config.TestInfo;

import java.sql.SQLException;

public class LoadTest extends TestBase {

    void runLoadTest() throws SQLException {
        for (final TestInfo testInfo : getTestConfigList()) {
            final LoadTestInfo loadTestInfo = testInfo.getLoadTestInfo();
            final java.sql.Connection connection = getConnection(testInfo);
            for (int i = 0; i < testInfo.getRepeats(); i++) {
                // TODO
                final String query1 = loadTestInfo.getQuery1();
            }
        }
    }
}
