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

package software.aws.performance;

import org.junit.jupiter.api.Test;
import java.sql.SQLException;

/**
 * Benchmark the driver's performance retrieving different amounts of data and data types.
 */
public abstract class DataTypePerformance {
    private static final int RUN_COUNT = 12;

    protected abstract PerformanceTestExecutor getPerformanceTestExecutor();

    protected abstract String getAllDataQuery();

    protected abstract String getNumberOfResultsQuery();

    protected abstract String getTransformNumberOfIntegersQuery();

    protected abstract String getTransformNumberOfStringsQuery();

    protected abstract String getBaseTestName();

    void runQuery(final String testName, final String query, final int runCount,
                  final PerformanceTestExecutor.RetrieveType retrieveType) {
        getPerformanceTestExecutor().runTest(testName, query, runCount, retrieveType);
    }

    @Test
    void testGetAllData() throws SQLException {
        runQuery(getBaseTestName() + "-GetAllDataTest", getAllDataQuery(), RUN_COUNT, PerformanceTestExecutor.RetrieveType.OBJECT);
    }

    @Test
    void testGetNumberOfResults() throws SQLException {
        runQuery(getBaseTestName() + "-GetNumberResultsTest", getNumberOfResultsQuery(), RUN_COUNT,
                PerformanceTestExecutor.RetrieveType.OBJECT);
    }

    @Test
    void testTransformNumberOfIntegers() throws SQLException {
        runQuery(getBaseTestName() + "-GetTransformNumberIntegersTest", getTransformNumberOfIntegersQuery(), RUN_COUNT,
                PerformanceTestExecutor.RetrieveType.INTEGER);
    }

    @Test
    void testTransformNumberOfStrings() throws SQLException {
        runQuery(getBaseTestName() + "-GetTransformNumberStringTest", getTransformNumberOfStringsQuery(), RUN_COUNT,
                PerformanceTestExecutor.RetrieveType.STRING);
    }
}
