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

import lombok.SneakyThrows;
import java.io.IOException;
import java.util.AbstractMap;

/**
 * Abstract class to handle performance test execution.
 */
public abstract class PerformanceTestExecutor {

    protected abstract Object execute(String query);

    protected abstract int retrieve(Object retrieveObject);

    protected abstract int retrieveString(Object retrieveObject);

    protected abstract int retrieveInteger(Object retrieveObject);

    /**
     * This function performs the test using the abstract functions.
     *
     * @param testName Name of the test.
     * @param query    Query to run.
     * @param runs     Number of times to execute.
     */
    @SneakyThrows
    public void runTest(final String testName, final String query, final int runs, final RetrieveType retrieveType) {

        final Metric retrievalMetric = new Metric();
        final Metric executionMetric = new Metric();
        for (int i = 0; i < runs; i++) {
            final long startExecuteTime = System.nanoTime();
            final Object data = execute(query);
            final long startRetrievalTime = System.nanoTime();
            executionMetric.trackExecutionTime(System.nanoTime() - startExecuteTime);
            final int rowCount;
            switch (retrieveType) {
                case STRING:
                    rowCount = retrieveString(data);
                    break;
                case INTEGER:
                    rowCount = retrieveInteger(data);
                    break;
                case OBJECT:
                    rowCount = retrieve(data);
                    break;
                default:
                    throw new Exception(String.format("Unknown retrieval type: %s", retrieveType.name()));
            }
            retrievalMetric.trackExecutionTime(System.nanoTime() - startRetrievalTime);
            if (i == 0) {
                retrievalMetric.setNumberOfRows(rowCount);
            }
        }
        handleMetrics(testName, new AbstractMap.SimpleEntry<>(retrievalMetric, executionMetric));

    }

    /**
     * Handle the performance test metrics.
     *
     * @param testName Name of the performance test.
     * @param metrics  The metric for executing the query and the metric for data retrieval.
     */
    private void handleMetrics(final String testName,
                               final AbstractMap.SimpleEntry<Metric, Metric> metrics) {
        final Metric retrievalMetric = metrics.getKey();
        final Metric executionMetric = metrics.getValue();
        try {
            PerformanceTestUtils.writeToCsv(executionMetric, retrievalMetric, testName);
        } catch (final IOException e) {
            System.out.println("Unable to save metrics to a csv file: " + e.getMessage());
        }
    }

    enum RetrieveType {
        OBJECT,
        STRING,
        INTEGER
    }
}
