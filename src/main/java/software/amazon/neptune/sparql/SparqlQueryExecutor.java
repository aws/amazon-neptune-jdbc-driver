/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.neptune.sparql;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneApacheHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.QueryExecutor;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import software.amazon.neptune.sparql.resultset.SparqlResultSet;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SparqlQueryExecutor extends QueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparqlQueryExecutor.class);
    private static final Object RDF_CONNECTION_LOCK = new Object();
    private static RDFConnection rdfConnection = null;
    private static SparqlConnectionProperties previousSparqlConnectionProperties = null;
    private static QueryExecution queryExecution = null;
    private final Object queryExecutionLock = new Object();
    private final SparqlConnectionProperties sparqlConnectionProperties;


    SparqlQueryExecutor(final SparqlConnectionProperties sparqlConnectionProperties) {
        this.sparqlConnectionProperties = sparqlConnectionProperties;
    }

    private static RDFConnectionRemoteBuilder createRDFBuilder(final SparqlConnectionProperties properties)
            throws SQLException, NeptuneSigV4SignerException {
        final RDFConnectionRemoteBuilder builder = RDFConnectionRemote.create();

        if (properties.containsKey(SparqlConnectionProperties.DESTINATION_KEY)) {
            builder.destination(properties.getDestination());
        }

        if (properties.containsKey(SparqlConnectionProperties.QUERY_ENDPOINT_KEY)) {
            builder.queryEndpoint(properties.getQueryEndpoint());
        }

        if (properties.containsKey(SparqlConnectionProperties.ACCEPT_HEADER_ASK_QUERY_KEY)) {
            builder.acceptHeaderAskQuery(properties.getAcceptHeaderAskQuery());
        }

        if (properties.containsKey(SparqlConnectionProperties.ACCEPT_HEADER_DATASET_KEY)) {
            builder.acceptHeaderDataset(properties.getAcceptHeaderDataset());
        }

        if (properties.containsKey(SparqlConnectionProperties.ACCEPT_HEADER_GRAPH_KEY)) {
            builder.acceptHeaderGraph(properties.getAcceptHeaderGraph());
        }

        if (properties.containsKey(SparqlConnectionProperties.ACCEPT_HEADER_QUERY_KEY)) {
            builder.acceptHeaderQuery(properties.getAcceptHeaderQuery());
        }

        if (properties.containsKey(SparqlConnectionProperties.ACCEPT_HEADER_SELECT_QUERY_KEY)) {
            builder.acceptHeaderSelectQuery(properties.getAcceptHeaderSelectQuery());
        }

        if (properties.containsKey(SparqlConnectionProperties.GSP_ENDPOINT_KEY)) {
            builder.gspEndpoint(properties.getGspEndpoint());
        }

        if (properties.containsKey(SparqlConnectionProperties.PARSE_CHECK_SPARQL_KEY)) {
            builder.parseCheckSPARQL(properties.getParseCheckSparql());
        }

        // https://github.com/aws/amazon-neptune-sparql-java-sigv4/blob/master/src/main/java/com/amazonaws/neptune/client/jena/NeptuneJenaSigV4Example.java
        if (properties.getAuthScheme() == AuthScheme.IAMSigV4) {

            final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
            final NeptuneApacheHttpSigV4Signer v4Signer =
                    new NeptuneApacheHttpSigV4Signer(properties.getRegion(), awsCredentialsProvider);

            final HttpClient v4SigningClient =
                    HttpClientBuilder.create().addInterceptorLast(new HttpRequestInterceptor() {

                        @Override
                        public void process(final HttpRequest req, final HttpContext ctx) throws HttpException {
                            if (req instanceof HttpUriRequest) {
                                final HttpUriRequest httpUriReq = (HttpUriRequest) req;
                                try {
                                    v4Signer.signRequest(httpUriReq);
                                } catch (final NeptuneSigV4SignerException e) {
                                    throw new HttpException("Problem signing the request: ", e);
                                }
                            } else {
                                throw new HttpException("Not an HttpUriRequest"); // this should never happen
                            }
                        }

                    }).build();

            properties.setHttpClient(v4SigningClient);
            builder.httpClient(v4SigningClient);

        } else if (properties.containsKey(SparqlConnectionProperties.HTTP_CLIENT_KEY)) {
            builder.httpClient(properties.getHttpClient());
        }

        if (properties.containsKey(SparqlConnectionProperties.HTTP_CONTEXT_KEY)) {
            builder.httpContext(properties.getHttpContext());
        }

        if (properties.containsKey(SparqlConnectionProperties.QUADS_FORMAT_KEY)) {
            builder.quadsFormat(properties.getQuadsFormat());
        }

        if (properties.containsKey(SparqlConnectionProperties.TRIPLES_FORMAT_KEY)) {
            builder.triplesFormat(properties.getTriplesFormat());
        }

        return builder;
    }

    /**
     * Function to close down the RDF connection.
     */
    public static void close() {
        synchronized (RDF_CONNECTION_LOCK) {
            if (rdfConnection != null) {
                rdfConnection.close();
                rdfConnection = null;
            }
        }
    }

    private RDFConnection getRdfConnection(final SparqlConnectionProperties sparqlConnectionProperties)
            throws SQLException {
        if (rdfConnection == null || !propertiesEqual(previousSparqlConnectionProperties, sparqlConnectionProperties)) {
            previousSparqlConnectionProperties = sparqlConnectionProperties;
            try {
                return createRDFBuilder(sparqlConnectionProperties).build();
            } catch (final NeptuneSigV4SignerException e) {
                // TODO AN-531 look into this exception handling for auth
                e.printStackTrace();
            }
        }
        return rdfConnection;
    }

    @Override
    public int getMaxFetchSize() {
        return 0;
    }

    /**
     * Verify that connection to database is functional.
     *
     * @param timeout Time in seconds to wait for the database operation used to validate the connection to complete.
     * @return true if the connection is valid, otherwise false.
     */
    @Override
    public boolean isValid(final int timeout) {
        try {
            final RDFConnection tempConn =
                    SparqlQueryExecutor.createRDFBuilder(sparqlConnectionProperties).build();
            final QueryExecution executeQuery = tempConn.query("SELECT * { ?s ?p ?o } LIMIT 0");
            // the 2nd parameter controls the timeout for the whole query execution
            executeQuery.setTimeout(timeout, TimeUnit.SECONDS, timeout, TimeUnit.SECONDS);
            executeQuery.execSelect();
            return true;
        } catch (final Exception e) {
            LOGGER.error("Connection to database returned an error:", e);
            return false;
        }
    }

    /**
     * Function to execute query.
     *
     * @param sparql    Query to execute.
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public ResultSet executeQuery(final String sparql, final Statement statement) throws SQLException {
        // TODO
        final Constructor<?> constructor;
        try {
            constructor = SparqlResultSet.class
                    .getConstructor(java.sql.Statement.class, SparqlResultSet.ResultSetInfoWithRows.class);
        } catch (final NoSuchMethodException e) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.INVALID_QUERY_EXPRESSION,
                    SqlError.QUERY_FAILED, e);
        }
        return runCancellableQuery(constructor, statement, sparql);
    }

    /**
     * Function to get tables.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @param tableName String table name with colon delimits.
     * @return java.sql.ResultSet object returned from query execution.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public ResultSet executeGetTables(final Statement statement, final String tableName) throws SQLException {
        return null;
    }

    /**
     * Function to get schema.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResulSet Object containing schemas.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public ResultSet executeGetSchemas(final Statement statement) throws SQLException {
        return null;
    }

    /**
     * Function to get catalogs.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing catalogs.
     */
    @Override
    public ResultSet executeGetCatalogs(final Statement statement) throws SQLException {
        return null;
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing table types.
     */
    @Override
    public ResultSet executeGetTableTypes(final Statement statement) throws SQLException {
        return null;
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @param nodes     String containing nodes to get schema for.
     * @return java.sql.ResultSet Object containing columns.
     */
    @Override
    public ResultSet executeGetColumns(final Statement statement, final String nodes) throws SQLException {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T runQuery(final String query) throws SQLException {
        // TODO
        synchronized (queryExecutionLock) {
            synchronized (RDF_CONNECTION_LOCK) {
                rdfConnection = getRdfConnection(sparqlConnectionProperties);
            }
            queryExecution = rdfConnection.query(query);
        }

        final org.apache.jena.query.ResultSet result = queryExecution.execSelect();
        final List<QuerySolution> rows = new ArrayList<>();
        final List<String> columns = result.getResultVars();

        while (result.hasNext()) {
            final QuerySolution querySolution = result.next();
            rows.add(querySolution);
        }

        final SparqlResultSet.ResultSetInfoWithRows sparqlResultSet =
                new SparqlResultSet.ResultSetInfoWithRows(result, rows, columns);

        synchronized (queryExecutionLock) {
            // close queryExecution first? or just set to null?
            queryExecution = null;
        }

        return (T) sparqlResultSet;
    }

    @Override
    protected void performCancel() throws SQLException {
        // TODO
        // store in class variable, check null/process whether to cancel or throw
        // synchronized block
        synchronized (queryExecutionLock) {
            if (queryExecution != null) {
                queryExecution.abort();
                queryExecution = null;
            }
        }
    }
}
