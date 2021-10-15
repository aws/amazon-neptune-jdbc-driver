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

package software.aws.neptune.common.gremlindatamodel;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.calcite.util.Pair;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.SigV4WebSocketChannelizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinEdgeTable;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinProperty;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinVertexTable;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SchemaHelperGremlinDataModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaHelperGremlinDataModel.class);
    private static final Map<Class<?>, String> TYPE_MAP = new HashMap<>();
    private static final String VERTEX_EDGES_LABEL_QUERY = "g.V().hasLabel('%s').%sE().label().dedup()";
    private static final String PROPERTIES_VALUE_QUERY = "g.%s().hasLabel('%s').values('%s').%s";
    private static final String PROPERTY_KEY_QUERY = "g.%s().hasLabel('%s').properties().key().dedup()";
    private static final String LABELS_QUERY = "g.%s().label().dedup()";
    private static final String IN_OUT_VERTEX_QUERY = "g.E().hasLabel('%s').project('in','out').by(inV().label()).by(outV().label()).dedup()";

    static {
        TYPE_MAP.put(String.class, "String");
        TYPE_MAP.put(Boolean.class, "Boolean");
        TYPE_MAP.put(Byte.class, "Byte");
        TYPE_MAP.put(Short.class, "Short");
        TYPE_MAP.put(Integer.class, "Integer");
        TYPE_MAP.put(Long.class, "Long");
        TYPE_MAP.put(Float.class, "Float");
        TYPE_MAP.put(Double.class, "Double");
        TYPE_MAP.put(Date.class, "Date");
    }

    private static String getAdjustedEndpoint(final String endpoint, final MetadataCache.PathType pathType) throws SQLException {
        if (pathType == MetadataCache.PathType.Bolt) {
            final String[] endpointSplit = endpoint.split(":");
            if ((endpointSplit.length != 3) || (!endpointSplit[1].startsWith("//"))) {
                throw SqlError.createSQLException(LOGGER, SqlState.CONNECTION_FAILURE, SqlError.INVALID_ENDPOINT, endpoint);
            }
            return endpointSplit[1].substring(2);
        } else {
            return endpoint;
        }
    }

    public static GremlinSchema getGraphSchema(final String endpoint, final int port, final boolean useIAM, final boolean useSsl,
                                               final MetadataCache.PathType pathType, ScanType scanType)
            throws SQLException {
        final String adjustedEndpoint = getAdjustedEndpoint(endpoint, pathType);
        // This gremlin utility requires that the SERVICE_REGION is set no matter what usage of IAM is being used.
        if (useIAM) {
            if (!System.getenv().containsKey("SERVICE_REGION")) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.MISSING_SERVICE_REGION);
            }
        }
        final Client client = getClient(adjustedEndpoint, port, useIAM, useSsl);
        return getSchema(client, scanType);
    }

    private static Client getClient(final String endpoint, final int port, final boolean useIam, final boolean useSsl) {
        final Cluster.Builder builder = Cluster.build();
        builder.addContactPoint(endpoint);
        builder.port(port);
        builder.enableSsl(useSsl);
        if (useIam) {
            builder.channelizer(SigV4WebSocketChannelizer.class);
        }
        final Cluster cluster = builder.create();
        final Client client = cluster.connect();
        client.init();
        return client;
    }

    private static GremlinSchema getSchema(final Client client, final ScanType scanType) throws SQLException {
        final ExecutorService executor = Executors.newFixedThreadPool(96,
                new ThreadFactoryBuilder().setNameFormat("RxSessionRunner-%d").setDaemon(true).build());
        try {
            final Future<List<GremlinVertexTable>> gremlinVertexTablesFuture = executor.submit(new RunGremlinQueryVertices(client, executor, scanType));
            final Future<List<GremlinEdgeTable>> gremlinEdgeTablesFuture = executor.submit(new RunGremlinQueryEdges(client, executor, scanType));
            final GremlinSchema gremlinSchema = new GremlinSchema(gremlinVertexTablesFuture.get(), gremlinEdgeTablesFuture.get());
            executor.shutdown();
            return gremlinSchema;
        } catch (final ExecutionException | InterruptedException e) {
            e.printStackTrace();
            executor.shutdown();
            throw new SQLException("Error occurred during schema collection. '" + e.getMessage() + "'.");
        }
    }

    private static String getType(final Set<Object> data) {
        final Set<String> types = new HashSet<>();
        for (final Object d : data) {
            types.add(TYPE_MAP.getOrDefault(d.getClass(), "String"));
        }
        if (types.size() == 1) {
            return types.iterator().next();
        }
        return "String";
    }

    public enum ScanType {
        First("First"),
        All("All");

        private final String stringValue;

        ScanType(@NonNull final String stringValue) {
            this.stringValue = stringValue;
        }

        /**
         * Converts case-insensitive string to enum value.
         *
         * @param in The case-insensitive string to be converted to enum.
         * @return The enum value if string is recognized as a valid value, otherwise null.
         */
        public static ScanType fromString(@NonNull final String in) {
            for (final ScanType scheme : ScanType.values()) {
                if (scheme.stringValue.equalsIgnoreCase(in)) {
                    return scheme;
                }
            }
            return null;
        }

        @Override
        public java.lang.String toString() {
            return this.stringValue;
        }
    }

    @AllArgsConstructor
    static
    class RunGremlinQueryVertices implements Callable<List<GremlinVertexTable>> {
        private final Client client;
        private final ExecutorService service;
        private final ScanType scanType;

        @Override
        public List<GremlinVertexTable> call() throws Exception {
            final List<Future<List<GremlinProperty>>> gremlinProperties = new ArrayList<>();
            final List<Future<List<String>>> gremlinVertexInEdgeLabels = new ArrayList<>();
            final List<Future<List<String>>> gremlinVertexOutEdgeLabels = new ArrayList<>();
            final List<String> labels = service.submit(new RunGremlinQueryLabels(true, client)).get();

            for (final String label : labels) {
                gremlinProperties.add(service.submit(
                        new RunGremlinQueryPropertiesList(true, label, client, scanType, service)));
                gremlinVertexInEdgeLabels.add(service.submit(new RunGremlinQueryVertexEdges(client, label, "in")));
                gremlinVertexOutEdgeLabels.add(service.submit(new RunGremlinQueryVertexEdges(client, label, "out")));
            }

            final List<GremlinVertexTable> gremlinVertexTables = new ArrayList<>();
            for (int i = 0; i < labels.size(); i++) {
                gremlinVertexTables.add(new GremlinVertexTable(labels.get(i), gremlinProperties.get(i).get(),
                        gremlinVertexInEdgeLabels.get(i).get(), gremlinVertexOutEdgeLabels.get(i).get()));
            }
            return gremlinVertexTables;
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryEdges implements Callable<List<GremlinEdgeTable>> {
        private final Client client;
        private final ExecutorService service;
        private final ScanType scanType;

        @Override
        public List<GremlinEdgeTable> call() throws Exception {
            final List<Future<List<GremlinProperty>>> futureTableColumns = new ArrayList<>();
            final List<Future<List<Pair<String, String>>>> inOutLabels = new ArrayList<>();
            final List<String> labels = service.submit(new RunGremlinQueryLabels(false, client)).get();

            for (final String label : labels) {
                futureTableColumns.add(service.submit(
                        new RunGremlinQueryPropertiesList(false, label, client, scanType, service)));
                inOutLabels.add(service.submit(new RunGremlinQueryInOutV(client, label)));
            }

            final List<GremlinEdgeTable> gremlinEdgeTables = new ArrayList<>();
            for (int i = 0; i < labels.size(); i++) {
                gremlinEdgeTables.add(new GremlinEdgeTable(labels.get(i), futureTableColumns.get(i).get(), inOutLabels.get(i).get()));
            }
            return gremlinEdgeTables;
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryVertexEdges implements Callable<List<String>> {
        private final Client client;
        private final String label;
        private final String direction;

        @Override
        public List<String> call() throws Exception {
            final String query = String.format(VERTEX_EDGES_LABEL_QUERY, label, direction);
            LOGGER.debug(String.format("Start %s%n", query));
            final ResultSet resultSet = client.submit(query);
            final List<String> labels = new ArrayList<>();
            resultSet.stream().iterator().forEachRemaining(it -> labels.add(it.getString()));
            LOGGER.debug(String.format("End %s%n", query));
            return labels;
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryPropertyType implements Callable<String> {
        private final boolean isVertex;
        private final String label;
        private final String property;
        private final Client client;
        private final ScanType strategy;

        @Override
        public String call() {
            final String query = String.format(PROPERTIES_VALUE_QUERY, isVertex ? "V" : "E", label, property,
                    strategy.equals(ScanType.First) ? "next(1)" : "toSet()");
            LOGGER.debug(String.format("Start %s%n", query));
            final ResultSet resultSet = client.submit(query);
            final Set<Object> data = new HashSet<>();
            resultSet.stream().iterator().forEachRemaining(r -> data.add(r.getObject()));
            LOGGER.debug(String.format("End %s%n", query));
            return getType(data);
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryPropertiesList implements Callable<List<GremlinProperty>> {
        private final boolean isVertex;
        private final String label;
        private final Client client;
        private final ScanType scanType;
        private final ExecutorService service;

        @Override
        public List<GremlinProperty> call() throws ExecutionException, InterruptedException {
            final String query = String.format(PROPERTY_KEY_QUERY, isVertex ? "V" : "E", label);
            LOGGER.debug(String.format("Start %s%n", query));
            final ResultSet resultSet = client.submit(query);
            final Iterator<Result> iterator = resultSet.stream().iterator();
            final List<String> properties = new ArrayList<>();
            final List<Future<String>> propertyTypes = new ArrayList<>();
            while (iterator.hasNext()) {
                final String property = iterator.next().getString();
                propertyTypes.add(service.submit(new RunGremlinQueryPropertyType(isVertex, label, property, client, scanType)));
                properties.add(property);
            }

            final List<GremlinProperty> columns = new ArrayList<>();
            for (int i = 0; i < properties.size(); i++) {
                columns.add(new GremlinProperty(properties.get(i), propertyTypes.get(i).get().toLowerCase()));
            }

            LOGGER.debug(String.format("End %s%n", query));
            return columns;
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryLabels implements Callable<List<String>> {
        private final boolean isVertex;
        private final Client client;

        @Override
        public List<String> call() {
            final String query = String.format(LABELS_QUERY, isVertex ? "V" : "E");
            LOGGER.debug(String.format("Start %s%n", query));
            final List<String> labels = new ArrayList<>();
            final ResultSet resultSet = client.submit(query);
            resultSet.stream().iterator().forEachRemaining(it -> labels.add(it.getString()));
            LOGGER.debug(String.format("End %s%n", query));
            return labels;
        }
    }

    @AllArgsConstructor
    static class RunGremlinQueryInOutV implements Callable<List<Pair<String, String>>> {
        private final Client client;
        private final String label;

        @Override
        public List<Pair<String, String>> call() {
            final String query = String.format(IN_OUT_VERTEX_QUERY, label);
            LOGGER.debug(String.format("Start %s%n", query));
            final List<Pair<String, String>> labels = new ArrayList<>();
            final ResultSet resultSet = client.submit(query);
            resultSet.stream().iterator().forEachRemaining(map -> {
                final Map<String, String> m = (Map<String, String>) map.getObject();
                m.forEach((key, value) -> labels.add(new Pair<>(key, value)));
            });
            LOGGER.debug(String.format("End %s%n", query));
            return labels;
        }
    }
}
