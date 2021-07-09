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

package software.amazon.neptune.sparql;

import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.NeptuneStatementTestHelper;
import software.amazon.neptune.sparql.mock.SparqlMockServer;
import java.sql.SQLException;
import java.util.Properties;

public class SparqlStatementTest extends SparqlStatementTestBase {
    private static final String HOSTNAME = "http://localhost";
    private static final String ENDPOINT = "mock";
    private static final String QUERY_ENDPOINT = "query";
    private static final int PORT = SparqlMockServer.port(); // Mock server dynamically generates port
    private static NeptuneStatementTestHelper neptuneStatementTestHelper;

    private static Properties sparqlProperties() {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.None); // set default to None
        properties.put(SparqlConnectionProperties.ENDPOINT_KEY, HOSTNAME);
        properties.put(SparqlConnectionProperties.PORT_KEY, PORT);
        properties.put(SparqlConnectionProperties.DATASET_KEY, ENDPOINT);
        properties.put(SparqlConnectionProperties.QUERY_ENDPOINT_KEY, QUERY_ENDPOINT);
        return properties;
    }

    /**
     * Function to start the mock server before testing.
     */
    @BeforeEach
    public void initializeMockServer() throws SQLException {
        SparqlMockServer.ctlBeforeEach();
        final RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create()
                .destination(SparqlMockServer.urlDataset())
                .queryEndpoint("/query")
                .updateEndpoint("/update");

        final UpdateRequest update =
                UpdateFactory.create(SparqlStatementTestBase.LONG_UPDATE);

        // load dataset in
        try (final RDFConnection conn = builder.build()) {
            conn.load("src/test/java/software/amazon/neptune/sparql/mock/sparql_mock_data.rdf");
            conn.update(update);
        }

        final java.sql.Connection connection =
                new SparqlConnection(
                        new SparqlConnectionProperties(sparqlProperties()));

        neptuneStatementTestHelper =
                new NeptuneStatementTestHelper(connection.createStatement(), LONG_QUERY, QUICK_QUERY);
    }

    /**
     * Function to tear down server after testing.
     */
    @AfterEach
    public void shutdownMockServer() {
        SparqlMockServer.ctlAfterEach();
    }

    @Test
    void testCancelQueryWithoutExecute() {
        neptuneStatementTestHelper.testCancelQueryWithoutExecute();
    }

    // TODO: Disabling this test due to query timing inconsistency across different machines leading to failed test.
    //  Will address this issue in ticket AN-597
    @Test
    @Disabled
    void testCancelQueryTwice() {
        neptuneStatementTestHelper.testCancelQueryTwice();
    }

    @Test
    void testCancelQueryAfterExecuteComplete() {
        neptuneStatementTestHelper.testCancelQueryAfterExecuteComplete();
    }
}
