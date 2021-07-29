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

import com.google.common.math.Quantiles;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Utility class for the performance tests.
 */
class PerformanceTestUtils {
    private static final String CSV_OUTPUT = "./performance-test-results-%s.csv";

    private static final String getCsvFile(final String testName) {
        return String.format(CSV_OUTPUT, testName);
    }

    /**
     * Write the performance test metrics to a CSV file.
     *
     * @param executionMetric The {@link Metric} tracking execution times.
     * @param retrievalMetric The {@link Metric} tracking retrieval times.
     * @param testName        Name of the performance test.
     * @throws IOException if an error occurred while creating the file.
     */
    static void writeToCsv(final Metric executionMetric, final Metric retrievalMetric, final String testName)
            throws IOException {
        final File newFile = new File(getCsvFile(testName));
        if (!newFile.exists()) {
            if (!newFile.createNewFile()) {
                System.out.println("Cannot create a new CSV file at the specified path.");
                return;
            } else {
                createNewCSVFile(testName);
            }
        }

        appendToCSVFile(executionMetric, retrievalMetric, testName);
    }

    /**
     * Append performance test metrics to the CSV.
     *
     * @param executionMetric The {@link Metric} tracking execution times.
     * @param retrievalMetric The {@link Metric} tracking retrieval times.
     * @param testName        Name of the performance test.
     */
    private static void appendToCSVFile(final Metric executionMetric, final Metric retrievalMetric,
                                        final String testName) {
        final List<Double> normalizedRetrievalTimes = PerformanceTestUtils
                .normalizeTimes(retrievalMetric);
        final Map<Integer, Double> percentiles = Quantiles.percentiles().indexes(90, 95, 99)
                .compute(normalizedRetrievalTimes);

        try (final Writer csv = new OutputStreamWriter(new FileOutputStream(getCsvFile(testName), true),
                StandardCharsets.UTF_8)) {
            final StringJoiner dataJoiner = new StringJoiner(",");
            dataJoiner.add(testName);
            dataJoiner.add(String.valueOf(retrievalMetric.getNumberOfRows()));
            dataJoiner.add(String
                    .valueOf(PerformanceTestUtils.toMillis(executionMetric.getMinExecutionTime())));
            dataJoiner.add(String
                    .valueOf(PerformanceTestUtils.toMillis(executionMetric.getMaxExecutionTime())));
            dataJoiner.add(String
                    .valueOf(PerformanceTestUtils.toMillis(executionMetric.calculateAverageExecutionTime())));
            dataJoiner.add(String
                    .valueOf(PerformanceTestUtils.toMillis(retrievalMetric.getMinExecutionTime())));
            dataJoiner.add(String
                    .valueOf(PerformanceTestUtils.toMillis(retrievalMetric.getMaxExecutionTime())));
            dataJoiner.add(String
                    .valueOf(PerformanceTestUtils.toMillis(retrievalMetric.calculateAverageExecutionTime())));
            dataJoiner.add(String.valueOf(percentiles.get(90)));
            dataJoiner.add(String.valueOf(percentiles.get(95)));
            dataJoiner.add(String.valueOf(percentiles.get(99)));

            csv.append(dataJoiner.toString());
            csv.append("\n");
        } catch (final IOException e) {
            System.out.println("Unable to save metrics to a csv file: " + e.getMessage());
        }
    }

    /**
     * Creates a new CSV file tracking the performance metrics and add headers to the new CSV file.
     */
    private static void createNewCSVFile(final String testName) {
        final StringJoiner joiner = new StringJoiner(",");
        try (final Writer csv = new OutputStreamWriter(new FileOutputStream(getCsvFile(testName)),
                StandardCharsets.UTF_8)) {
            joiner.add("Performance Test");
            joiner.add("Number of Rows");
            joiner.add("Min Execution Time");
            joiner.add("Max Execution Time");
            joiner.add("Average Execution Time");
            joiner.add("Min Retrieval Time");
            joiner.add("Max Retrieval Time");
            joiner.add("Average Retrieval Time");
            joiner.add("P90");
            joiner.add("P95");
            joiner.add("P99");
            csv
                    .append(joiner.toString())
                    .append("\n");
        } catch (final IOException e) {
            System.out.println("Unable to save metrics to a csv file: " + e.getMessage());
        }
    }

    /**
     * Normalize the running times.
     *
     * @param metric the performance test metric containing a list of running times.
     * @return the normalized running times.
     */
    private static List<Double> normalizeTimes(final Metric metric) {
        final List<Double> normalizedRunningTimes = new ArrayList<>(metric.getExecutionTimes());
        normalizedRunningTimes.remove(metric.getMaxExecutionTime());
        normalizedRunningTimes.remove(metric.getMinExecutionTime());
        return normalizedRunningTimes.parallelStream().map(d -> d / 1000000).collect(Collectors.toList());
    }

    /**
     * Converts nanoseconds to milliseconds. Not using {@link java.util.concurrent.TimeUnit} to
     * prevent truncation.
     *
     * @param nanoseconds The nanoseconds to convert.
     * @return milliseconds.
     */
    private static double toMillis(final double nanoseconds) {
        return nanoseconds / 1000000;
    }
}
