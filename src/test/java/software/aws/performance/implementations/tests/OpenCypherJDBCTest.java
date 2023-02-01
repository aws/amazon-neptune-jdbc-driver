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

package software.aws.performance.implementations.tests;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import software.aws.performance.DataTypePerformance;
import software.aws.performance.PerformanceTestExecutor;
import software.aws.performance.implementations.executors.OpenCypherJDBCExecutor;

import static software.aws.performance.implementations.PerformanceTestConstants.OPENCYPHER_ALL_DATA_LIMIT_QUERY;
import static software.aws.performance.implementations.PerformanceTestConstants.OPENCYPHER_ALL_DATA_QUERY;
import static software.aws.performance.implementations.PerformanceTestConstants.OPENCYPHER_NUMBER_QUERY;
import static software.aws.performance.implementations.PerformanceTestConstants.OPENCYPHER_STRING_QUERY;

@Disabled
public class OpenCypherJDBCTest extends DataTypePerformance {

    @Override
    @SneakyThrows
    protected PerformanceTestExecutor getPerformanceTestExecutor() {
        return new OpenCypherJDBCExecutor();
    }

    @Override
    protected String getAllDataQuery() {
        return OPENCYPHER_ALL_DATA_QUERY;
    }

    @Override
    protected String getNumberOfResultsQuery() {
        return OPENCYPHER_ALL_DATA_LIMIT_QUERY;
    }

    @Override
    // Need to grab specific properties of certain nodes.
    protected String getTransformNumberOfIntegersQuery() {
        return OPENCYPHER_NUMBER_QUERY;
    }

    @Override
    // Need to grab specific properties of certain nodes.
    protected String getTransformNumberOfStringsQuery() {
        return OPENCYPHER_STRING_QUERY;
    }

    @Override
    protected String getBaseTestName() {
        return "OpenCypherJDBC";
    }
}
