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

package software.aws.performance.implementations.executors;

import lombok.SneakyThrows;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdfconnection.RDFConnection;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import software.aws.neptune.sparql.SparqlConnectionProperties;
import software.aws.neptune.sparql.SparqlQueryExecutor;
import software.aws.performance.PerformanceTestExecutor;
import software.aws.performance.implementations.PerformanceTestConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SparqlBaselineExecutor extends PerformanceTestExecutor {
    private final RDFConnection rdfConnection;
    private QueryExecution queryExecution;

    /**
     * Constructor for SparqlBaselineExecutor.
     */
    @SneakyThrows
    public SparqlBaselineExecutor() {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, PerformanceTestConstants.AUTH_SCHEME);
        properties.put(SparqlConnectionProperties.ENDPOINT_KEY, PerformanceTestConstants.SPARQL_ENDPOINT);
        properties.put(SparqlConnectionProperties.PORT_KEY, PerformanceTestConstants.PORT);
        properties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, PerformanceTestConstants.SPARQL_QUERY);
        final SparqlConnectionProperties sparqlConnectionProperties = new SparqlConnectionProperties(properties);
        rdfConnection = SparqlQueryExecutor.createRDFBuilder(sparqlConnectionProperties).build();
    }

    /**
     * We are testing SELECT queries only
     */
    @Override
    @SneakyThrows
    protected Object execute(final String query) {
        final Object result;
        queryExecution = rdfConnection.query(query);
        result = queryExecution.execSelect();
        return result;
    }

    @Override
    @SneakyThrows
    protected int retrieve(final Object retrieveObject) {
        if (!(retrieveObject instanceof ResultSet)) {
            throw new Exception("Error: expected a ResultSet for data retrieval.");
        }

        int rowCount = 0;
        final ResultSet result = (ResultSet) retrieveObject;
        final List<QuerySolution> selectRows = new ArrayList<>();
        final List<String> columns = result.getResultVars();

        while (result.hasNext()) {
            rowCount++;
            final QuerySolution row = result.next();
            selectRows.add(row);
            for (final String column : columns) {
                row.get(column);
            }
        }
        // Need to close QueryExecution explicitly, else it remains open and will hit the max open HTTP client.
        // https://stackoverflow.com/questions/55368848/construct-query-hangs-in-fuseki-jena-after-executing-6-time
        queryExecution.close();
        return rowCount;
    }

    @Override
    @SneakyThrows
    protected int retrieveString(final Object retrieveObject) {
        if (!(retrieveObject instanceof ResultSet)) {
            throw new Exception("Error: expected a ResultSet for data retrieval.");
        }

        int rowCount = 0;
        final ResultSet result = (ResultSet) retrieveObject;
        final List<QuerySolution> selectRows = new ArrayList<>();
        final List<String> columns = result.getResultVars();

        while (result.hasNext()) {
            rowCount++;
            final QuerySolution row = result.next();
            selectRows.add(row);
            for (final String column : columns) {
                final RDFNode node = row.get(column);
                if (node.isLiteral()) {
                    final Literal literal = node.asLiteral();
                    literal.getLexicalForm();
                } else {
                    node.toString();
                }
            }
        }
        queryExecution.close();
        return rowCount;
    }

    @Override
    @SneakyThrows
    protected int retrieveInteger(final Object retrieveObject) {
        if (!(retrieveObject instanceof ResultSet)) {
            throw new Exception("Error: expected a ResultSet for data retrieval.");
        }
        int rowCount = 0;
        final ResultSet result = (ResultSet) retrieveObject;
        final List<QuerySolution> selectRows = new ArrayList<>();
        final List<String> columns = result.getResultVars();

        while (result.hasNext()) {
            rowCount++;
            final QuerySolution row = result.next();
            selectRows.add(row);
            for (final String column : columns) {
                final RDFNode node = row.get(column);
                if (node.isLiteral()) {
                    final Literal literal = node.asLiteral();
                    if (!(literal.getValue() instanceof Number)) {
                        Integer.parseInt(literal.getLexicalForm());
                    }
                } else {
                    Integer.parseInt(node.toString());
                }
            }
        }
        queryExecution.close();
        return rowCount;
    }
}
