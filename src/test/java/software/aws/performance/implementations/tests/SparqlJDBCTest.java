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

package software.aws.performance.implementations.tests;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import software.aws.performance.DataTypePerformance;
import software.aws.performance.PerformanceTestExecutor;
import software.aws.performance.implementations.executors.SparqlJDBCExecutor;

import static software.aws.performance.implementations.PerformanceTestConstants.LIMIT_COUNT;

@Disabled
public class SparqlJDBCTest extends DataTypePerformance {

    @Override
    @SneakyThrows
    protected PerformanceTestExecutor getPerformanceTestExecutor() {
        return new SparqlJDBCExecutor();
    }

    @Override
    // Get all airport data
    protected String getAllDataQuery() {
        return "SELECT ?s ?p ?o {?s ?p ?o}";
    }

    @Override
    protected String getNumberOfResultsQuery() {
        return String.format("%s LIMIT %d", getAllDataQuery(), LIMIT_COUNT);
    }

    @Override
    // Need to grab specific properties of certain nodes.
    protected String getTransformNumberOfIntegersQuery() {
        return "PREFIX prop:  <http://kelvinlawrence.net/air-routes/datatypeProperty/> " +
                "PREFIX class: <http://kelvinlawrence.net/air-routes/class/> " +
                "SELECT ?o " +
                "WHERE { " +
                "     ?s a class:Airport . " +
                "     ?s prop:elev ?o " +
                "} ";
    }

    @Override
    // Need to grab specific properties of certain nodes.
    protected String getTransformNumberOfStringsQuery() {
        return "PREFIX prop:  <http://kelvinlawrence.net/air-routes/datatypeProperty/> " +
                "PREFIX class: <http://kelvinlawrence.net/air-routes/class/> " +
                "SELECT ?s ?o " +
                "WHERE { " +
                "    ?s a class:Airport . " +
                "    ?s prop:code ?o " +
                "} ";
    }

    @Override
    protected String getBaseTestName() {
        return "SparqlJDBC";
    }
}
