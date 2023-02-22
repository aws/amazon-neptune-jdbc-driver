/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class tracking the execution time in nanoseconds from the performance tests.
 */
public class Metric {
    private final List<Double> executionTimes = new ArrayList<>();
    private int numberOfRows;

    /**
     * Gets the maximum execution time.
     *
     * @return the maximum execution time.
     */
    double getMaxExecutionTime() {
        return Collections.max(executionTimes);
    }

    /**
     * Gets the minimum execution time.
     *
     * @return the minimum execution time.
     */
    double getMinExecutionTime() {
        return Collections.min(executionTimes);
    }

    /**
     * Gets the execution times.
     *
     * @return the list of execution times.
     */
    List<Double> getExecutionTimes() {
        return executionTimes;
    }

    /**
     * Gets the total number of rows in the result set.
     *
     * @return the total number of rows in the result set.
     */
    int getNumberOfRows() {
        return numberOfRows;
    }

    /**
     * Sets the total number of rows in the result set.
     *
     * @param numberOfRows The total number of rows in the result set.
     */
    void setNumberOfRows(final int numberOfRows) {
        this.numberOfRows = numberOfRows;
    }

    /**
     * Track the execution time of the current iteration in the performance test.
     *
     * @param executionTime The execution time of an iteration in the performance test.
     */
    void trackExecutionTime(final double executionTime) {
        executionTimes.add(executionTime);
    }

    /**
     * Calculate the average execution time of the test over all the runs.
     *
     * @return the average execution time.
     */
    double calculateAverageExecutionTime() {
        final List<Double> normalizedExecutionTimes = new ArrayList<>(executionTimes);
        // Remove two extremes from each end
        normalizedExecutionTimes.remove(this.getMaxExecutionTime());
        normalizedExecutionTimes.remove(this.getMinExecutionTime());
        normalizedExecutionTimes.remove(this.getMaxExecutionTime());
        normalizedExecutionTimes.remove(this.getMinExecutionTime());
        return normalizedExecutionTimes
                .parallelStream()
                .collect(Collectors.averagingDouble(Double::doubleValue));
    }
}
