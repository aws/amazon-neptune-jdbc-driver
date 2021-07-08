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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneApacheHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import lombok.SneakyThrows;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.atlas.iterator.PeekIterator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.QueryType;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.rdfconnection.RDFConnectionRemoteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.QueryExecutor;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
import software.amazon.neptune.common.ResultSetInfoWithoutRows;
import software.amazon.neptune.common.gremlindatamodel.resultset.ResultSetGetTables;
import software.amazon.neptune.sparql.resultset.SparqlAskResultSet;
import software.amazon.neptune.sparql.resultset.SparqlResultSetGetCatelogs;
import software.amazon.neptune.sparql.resultset.SparqlResultSetGetColumns;
import software.amazon.neptune.sparql.resultset.SparqlResultSetGetSchemas;
import software.amazon.neptune.sparql.resultset.SparqlResultSetGetTableTypes;
import software.amazon.neptune.sparql.resultset.SparqlResultSetGetTables;
import software.amazon.neptune.sparql.resultset.SparqlSelectResultSet;
import software.amazon.neptune.sparql.resultset.SparqlTriplesResultSet;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
            throws SQLException {
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

        if (properties.containsKey(SparqlConnectionProperties.ACCEPT_HEADER_QUERY_KEY)) {
            builder.acceptHeaderQuery(properties.getAcceptHeaderQuery());
        }

        if (properties.containsKey(SparqlConnectionProperties.ACCEPT_HEADER_SELECT_QUERY_KEY)) {
            builder.acceptHeaderSelectQuery(properties.getAcceptHeaderSelectQuery());
        }

        if (properties.containsKey(SparqlConnectionProperties.PARSE_CHECK_SPARQL_KEY)) {
            builder.parseCheckSPARQL(properties.getParseCheckSparql());
        }

        if (properties.getAuthScheme() == AuthScheme.IAMSigV4) {
            builder.httpClient(createV4SigningClient(properties));
        } else if (properties.containsKey(SparqlConnectionProperties.HTTP_CLIENT_KEY)) {
            builder.httpClient(properties.getHttpClient());
        }

        if (properties.containsKey(SparqlConnectionProperties.HTTP_CONTEXT_KEY)) {
            builder.httpContext(properties.getHttpContext());
        }

        return builder;
    }

    // https://github.com/aws/amazon-neptune-sparql-java-sigv4/blob/master/src/main/java/com/amazonaws/neptune/client/jena/NeptuneJenaSigV4Example.java
    private static HttpClient createV4SigningClient(final SparqlConnectionProperties properties) throws SQLException {
        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        final NeptuneApacheHttpSigV4Signer v4Signer;
        final HttpClient v4SigningClient;

        try {
            v4Signer = new NeptuneApacheHttpSigV4Signer(properties.getRegion(), awsCredentialsProvider);
            v4SigningClient =
                    HttpClientBuilder.create().addInterceptorLast(new HttpRequestInterceptor() {

                        @SneakyThrows
                        @Override
                        public void process(final HttpRequest req, final HttpContext ctx) {
                            if (req instanceof HttpUriRequest) {
                                final HttpUriRequest httpUriReq = (HttpUriRequest) req;
                                try {
                                    v4Signer.signRequest(httpUriReq);
                                } catch (final NeptuneSigV4SignerException e) {
                                    throw SqlError.createSQLException(LOGGER,
                                            SqlState.INVALID_AUTHORIZATION_SPECIFICATION,
                                            SqlError.CONN_FAILED, e);
                                }
                            } else {
                                throw SqlError.createSQLException(LOGGER,
                                        SqlState.INVALID_AUTHORIZATION_SPECIFICATION,
                                        SqlError.UNSUPPORTED_REQUEST, "Not an HttpUriRequest");
                            }
                        }

                    }).build();

        } catch (final NeptuneSigV4SignerException e) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.INVALID_AUTHORIZATION_SPECIFICATION,
                    SqlError.CONN_FAILED, e);
        }
        return v4SigningClient;
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
            return createRDFBuilder(sparqlConnectionProperties).build();
        }
        return rdfConnection;
    }

    /**
     * Function to return max fetch size.
     *
     * @return Max fetch size (Integer max value).
     */
    @Override
    public int getMaxFetchSize() {
        return Integer.MAX_VALUE;
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
            // The 2nd parameter controls the timeout for the whole query execution.
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
        final Constructor<?> constructor = createConstructorBasedOnQueryType(sparql);
        return runCancellableQuery(constructor, statement, sparql);
    }

    /**
     * Private function to get constructor based on the given query type
     */
    private Constructor<?> createConstructorBasedOnQueryType(final String sparql) throws SQLException {
        final Constructor<?> constructor;
        try {
            final Query query = QueryFactory.create(sparql);
            switch (query.queryType()) {
                case SELECT:
                    constructor = SparqlSelectResultSet.class
                            .getConstructor(java.sql.Statement.class,
                                    SparqlSelectResultSet.ResultSetInfoWithRows.class);
                    break;
                case ASK:
                    constructor = SparqlAskResultSet.class
                            .getConstructor(java.sql.Statement.class, SparqlAskResultSet.ResultSetInfoWithRows.class);
                    break;
                case CONSTRUCT:
                case DESCRIBE:
                    constructor = SparqlTriplesResultSet.class
                            .getConstructor(java.sql.Statement.class,
                                    SparqlTriplesResultSet.ResultSetInfoWithRows.class);
                    break;
                default:
                    throw SqlError
                            .createSQLException(LOGGER, SqlState.INVALID_QUERY_EXPRESSION, SqlError.INVALID_QUERY);
            }
        } catch (final Exception e) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.INVALID_QUERY_EXPRESSION,
                    SqlError.QUERY_FAILED, e);
        }
        return constructor;
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
    public java.sql.ResultSet executeGetTables(final Statement statement, final String tableName) throws SQLException {
        return new SparqlResultSetGetTables(statement, new ArrayList<>(),
                new ResultSetInfoWithoutRows(0, ResultSetGetTables.getColumns()));
    }

    /**
     * Function to get schema.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing schemas.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    @Override
    public java.sql.ResultSet executeGetSchemas(final Statement statement) throws SQLException {
        return new SparqlResultSetGetSchemas(statement);
    }

    /**
     * Function to get catalogs.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing catalogs.
     */
    @Override
    public java.sql.ResultSet executeGetCatalogs(final Statement statement) throws SQLException {
        return new SparqlResultSetGetCatelogs(statement);
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @return java.sql.ResultSet Object containing table types.
     */
    @Override
    public java.sql.ResultSet executeGetTableTypes(final Statement statement) throws SQLException {
        return new SparqlResultSetGetTableTypes(statement);
    }

    /**
     * Function to get table types.
     *
     * @param statement java.sql.Statement Object required for result set.
     * @param nodes     String containing nodes to get schema for.
     * @return java.sql.ResultSet Object containing columns.
     */
    @Override
    public java.sql.ResultSet executeGetColumns(final Statement statement, final String nodes) throws SQLException {
        return new SparqlResultSetGetColumns(statement, new ArrayList<>(),
                new ResultSetInfoWithoutRows(0, ResultSetGetTables.getColumns()));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T runQuery(final String query) throws SQLException {
        synchronized (queryExecutionLock) {
            synchronized (RDF_CONNECTION_LOCK) {
                rdfConnection = getRdfConnection(sparqlConnectionProperties);
            }
            queryExecution = rdfConnection.query(query);
        }

        final QueryType queryType = queryExecution.getQuery().queryType();
        final Object sparqlResultSet = getResultSetBasedOnQueryType(queryType);

        synchronized (queryExecutionLock) {
            queryExecution.close();
            queryExecution = null;
        }

        return (T) sparqlResultSet;
    }

    /**
     * Private function to get result set based on the given query type
     */
    private Object getResultSetBasedOnQueryType(final QueryType queryType) throws SQLException {
        final Object sparqlResultSet;
        switch (queryType) {
            case SELECT:
                final org.apache.jena.query.ResultSet selectResult = queryExecution.execSelect();
                sparqlResultSet = getSelectResultSet(selectResult);
                break;
            case ASK:
                sparqlResultSet = new SparqlAskResultSet.ResultSetInfoWithRows(queryExecution.execAsk());
                break;
            case CONSTRUCT:
                final PeekIterator<Triple> constructResult = PeekIterator.create(queryExecution.execConstructTriples());
                sparqlResultSet = getTriplesResultSet(constructResult);
                break;
            case DESCRIBE:
                final PeekIterator<Triple> describeResult = PeekIterator.create(queryExecution.execDescribeTriples());
                sparqlResultSet = getTriplesResultSet(describeResult);
                break;
            default:
                throw SqlError
                        .createSQLException(LOGGER, SqlState.INVALID_QUERY_EXPRESSION, SqlError.INVALID_QUERY);
        }
        return sparqlResultSet;
    }

    /**
     * Private function to get select result set
     */
    private Object getSelectResultSet(final org.apache.jena.query.ResultSet selectResult) {
        final List<QuerySolution> selectRows = new ArrayList<>();
        final List<String> columns = selectResult.getResultVars();

        final List<String> tempColumns = new ArrayList<>(columns);
        final Map<String, Object> tempColumnType = new LinkedHashMap<>();

        // TODO: Revisit type promotion in performance testing ticket
        while (selectResult.hasNext()) {
            final QuerySolution row = selectResult.next();
            selectRows.add(row);
            final Iterator<String> tempColumnIterator = tempColumns.iterator();
            while (tempColumnIterator.hasNext()) {
                final String column = tempColumnIterator.next();
                final RDFNode rdfNode = row.get(column);
                if (rdfNode == null) {
                    continue;
                }
                final Node node = rdfNode.asNode();
                getColumnType(tempColumnType, tempColumnIterator, node, column);
            }
        }

        // Create new map to return column type.
        final Map<String, Object> selectColumnType = new LinkedHashMap<>();
        columns.forEach(c -> selectColumnType.put(c, tempColumnType.getOrDefault(c, String.class)));

        return new SparqlSelectResultSet.ResultSetInfoWithRows(selectRows, columns,
                new ArrayList<>(selectColumnType.values()));
    }

    /**
     * Private function to get Triples result set
     */
    private Object getTriplesResultSet(final PeekIterator<Triple> triplesResult) throws SQLException {
        final List<Triple> describeRows = new ArrayList<>();

        final List<String> tempColumns = new ArrayList<>(SparqlTriplesResultSet.TRIPLES_COLUMN_LIST);
        final Map<String, Object> tempColumnType = new LinkedHashMap<>();

        while (triplesResult.hasNext()) {
            final Triple row = triplesResult.next();
            describeRows.add(row);
            final Iterator<String> tempColumnIterator = tempColumns.iterator();
            while (tempColumnIterator.hasNext()) {
                final String column = tempColumnIterator.next();
                final Node node = getNodeFromColumnName(row, column);
                if (node == null) {
                    continue;
                }
                getColumnType(tempColumnType, tempColumnIterator, node, column);
            }
        }

        final Map<String, Object> triplesColumnType = new LinkedHashMap<>();
        SparqlTriplesResultSet.TRIPLES_COLUMN_LIST
                .forEach(c -> triplesColumnType.put(c, tempColumnType.getOrDefault(c, String.class)));

        return new SparqlTriplesResultSet.ResultSetInfoWithRows(describeRows,
                new ArrayList<>(triplesColumnType.values()));
    }

    /**
     * Private function to get node type from result set
     */
    private void getColumnType(final Map<String, Object> tempColumnType, final Iterator<String> tempColumnIterator,
                               final Node node, final String column) {
        if (node == null) {
            return;
        }
        final Object nodeType = node.isLiteral() ? node.getLiteral().getDatatype() : node.getClass();

        if (!tempColumnType.containsKey(column)) {
            tempColumnType.put(column, nodeType);
            // For Node, the resource type is org.apache.jena.graph.Node_URI instead of org.apache.jena.rdf.model.impl.ResourceImpl
            // Another possibility is to remove if is org.apache.jena.graph.Node_URI type.
        } else if (nodeType == null || !nodeType.equals(tempColumnType.get(column))) {
            tempColumnType.put(column, String.class);
            // Remove column from list if it is string type.
            tempColumnIterator.remove();
        }
    }

    /**
     * Private function to get Node from Triples result set column label
     */
    private Node getNodeFromColumnName(final Triple row, final String column) throws SQLException {
        final Node node;
        switch (column) {
            case SparqlTriplesResultSet.TRIPLES_COLUMN_LABEL_PREDICATE:
                node = row.getPredicate();
                break;
            case SparqlTriplesResultSet.TRIPLES_COLUMN_LABEL_SUBJECT:
                node = row.getSubject();
                break;
            case SparqlTriplesResultSet.TRIPLES_COLUMN_LABEL_OBJECT:
                node = row.getObject();
                break;
            default:
                throw SqlError
                        .createSQLException(LOGGER, SqlState.DATA_EXCEPTION, SqlError.INVALID_COLUMN_LABEL,
                                column);
        }

        return node;
    }

    @Override
    protected void performCancel() throws SQLException {
        synchronized (queryExecutionLock) {
            if (queryExecution != null) {
                queryExecution.abort();
                // TODO: Check in later tickets if adding close() affects anything or if we need any additional guards,
                //  as its implementation does have null checks
                queryExecution.close();
                queryExecution = null;
            }
        }
    }
}
