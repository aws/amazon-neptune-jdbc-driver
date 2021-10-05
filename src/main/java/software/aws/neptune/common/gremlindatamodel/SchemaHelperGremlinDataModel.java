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

import com.amazonaws.services.neptune.NeptuneExportCli;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.schema.SchemaConfig;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableColumn;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableConfig;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableRelationship;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.neptune.gremlin.GremlinQueryExecutor;
import software.aws.neptune.jdbc.utilities.SqlError;
import software.aws.neptune.jdbc.utilities.SqlState;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class SchemaHelperGremlinDataModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaHelperGremlinDataModel.class);

    /**
     * Function to get graph schema and return list of NodeColumnInfo describing it.
     *
     * @param endpoint       Endpoint to connect to.
     * @param nodes          Nodes to use if only single table is targeted.
     * @param nodeSchemaList List of GraphSchema for nodes.
     * @param edgeSchemaList List of GraphSchema for edges.
     * @throws SQLException Thrown if an error is encountered.
     */
    public static void getGraphSchema(final String endpoint, final String nodes, final boolean useIAM,
                                      final MetadataCache.PathType pathType, final List<GraphSchema> nodeSchemaList,
                                      final List<GraphSchema> edgeSchemaList, final int port)
            throws SQLException, IOException {
        // Create unique directory if doesn't exist
        // If does exist, delete current contents
        final String directory = createUniqueDirectoryForThread();

        // Run process
        final List<String> outputFiles = runGremlinSchemaGrabber(endpoint, nodes, directory, useIAM, pathType, port);

        // Validate to see if files are json
        for (final String file : outputFiles) {
            parseFile(file, nodeSchemaList, edgeSchemaList);
        }

        // Clean up
        try {
            deleteDirectoryIfExists(Paths.get(directory));
        } catch (final IOException ignored) {
        }
    }

    @VisibleForTesting
    static String createUniqueDirectoryForThread() throws SQLException, IOException {
        // Thread id is unique, so use it to create output directory.
        // Before output directory is created, check if it exists and delete contents if it does.
        final Path path = Files.createTempDirectory(String.format("%d", Thread.currentThread().getId()));
        LOGGER.info(String.format("Creating directory '%s'", path.toString()));
        final File outputDirectory = new File(path.toAbsolutePath().toString());
        if (!outputDirectory.exists()) {
            if (!outputDirectory.mkdirs()) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_FAILURE,
                        SqlError.FAILED_TO_CREATE_DIRECTORY);
            }
        }
        return path.toString();
    }

    @VisibleForTesting
    static void deleteDirectoryIfExists(final Path root) throws IOException {
        if (!root.toFile().exists()) {
            return;
        }
        Files.walk(root)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @VisibleForTesting
    static List<String> getOutputFiles(final String root) throws IOException {
        return Files.walk(Paths.get(root)).filter(Files::isRegularFile).map(Path::toString)
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    private static List<String> runGremlinSchemaGrabber(final String endpoint, final String nodes,
                                                        final String outputPath, final boolean useIAM,
                                                        final MetadataCache.PathType pathType,
                                                        final int port)
            throws SQLException {
        final String adjustedEndpoint;
        if (pathType == MetadataCache.PathType.Bolt) {
            final String[] endpointSplit = endpoint.split(":");
            if ((endpointSplit.length != 3) || (!endpointSplit[1].startsWith("//"))) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_FAILURE,
                        SqlError.INVALID_ENDPOINT, endpoint);
            }
            adjustedEndpoint = endpointSplit[1].substring(2);
        } else {
            adjustedEndpoint = endpoint;
        }

        // Setup arguments
        final List<String> arguments = new LinkedList<>();
        arguments.add("create-pg-config");
        arguments.add("-e");
        arguments.add(adjustedEndpoint);
        arguments.add("-d");
        arguments.add(outputPath);
        arguments.add("-p");
        arguments.add(String.format("%d", port));

        // This gremlin utility requires that the SERVICE_REGION is set no matter what usage of IAM is being used.
        if (useIAM) {
            if (!System.getenv().containsKey("SERVICE_REGION")) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.MISSING_SERVICE_REGION);
            }
            arguments.add("--use-iam-auth");
        }

        if (nodes != null && !nodes.isEmpty()) {
            final String[] nodeSplit = nodes.split(":");
            for (final String node : nodeSplit) {
                arguments.add("-nl");
                arguments.add(node);
            }
        }

        try {
            NeptuneExportCli.main(arguments.toArray(new String[0]));
            return getOutputFiles(outputPath);
        } catch (final Exception e) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_FAILURE,
                    SqlError.FAILED_TO_RUN_SCHEMA_EXPORT, e);
        }
    }

    @VisibleForTesting
    static void parseFile(final String filePath,
                          final List<GraphSchema> nodeSchemaList,
                          final List<GraphSchema> edgeSchemaList) {
        LOGGER.info(String.format("Parsing file '%s'", filePath));
        try {
            final String jsonString = new String(Files.readAllBytes(Paths.get(filePath).toAbsolutePath()));
            if (jsonString.isEmpty()) {
                throw new Exception(String.format("Schema file '%s' is empty.", filePath));
            }
            final ObjectMapper mapper = new ObjectMapper();
            final Map<String, List<Map<String, Object>>> nodesAndEdges = mapper.readValue(jsonString, HashMap.class);
            if (!nodesAndEdges.containsKey("nodes")) {
                throw new Exception("Schema file does not contain the 'node' key.");
            }

            // Get node labels and properties.
            parseGraphSchema("nodes", nodesAndEdges, nodeSchemaList);

            if (!nodesAndEdges.containsKey("edges")) {
                LOGGER.warn("Schema file does not contain the 'edge' key. Graph has no edges.");
            } else {
                parseGraphSchema("edges", nodesAndEdges, edgeSchemaList);
            }
        } catch (final Exception e) {
            LOGGER.error(e.getMessage());
        }
    }

    private static void parseGraphSchema(final String key, final Map<String, List<Map<String, Object>>> nodesAndEdges,
                                         final List<GraphSchema> graphSchemaList)
            throws Exception {
        for (final Map<String, Object> node : nodesAndEdges.get(key)) {
            if (!node.keySet().equals(ImmutableSet.of("label", "properties"))) {
                throw new Exception(
                        String.format("Schema under '%s' key does not contain 'label' and/or 'properties' keys", key));
            }
            final List<String> labels = new ArrayList<>();
            try {
                labels.addAll(getValueCheckType(node, "label", ArrayList.class));
            } catch (final Exception ignored) {
                labels.add(getValueCheckType(node, "label", String.class));
            }
            final List<Map<String, Object>> properties =
                    getValueCheckType(node, "properties", ArrayList.class);
            for (final Map<String, Object> property : properties) {
                if (!property.keySet()
                        .equals(ImmutableSet.of("property", "dataType", "isMultiValue", "isNullable"))) {
                    throw new Exception(
                            "Properties does not contain 'property', 'dataType', 'isMultiValue', and/or 'isNullable' keys");
                }
            }
            graphSchemaList.add(new GraphSchema(labels, properties));
        }
    }

    /**
     * Function to get value from map and check the type before returning it.
     *
     * @param map           Map to get result from.
     * @param key           Key that Object exists in map under.
     * @param expectedClass Expected type of Object from map.
     * @param <T>           Template type to cast Object to.
     * @return Object casted for specific type.
     * @throws Exception Throws an exception if the type does not match.
     */
    @SuppressWarnings("unchecked")
    private static <T> T getValueCheckType(final Map map, final String key, final Class<?> expectedClass)
            throws Exception {
        final Object obj = map.get(key);
        if (!(obj.getClass().equals(expectedClass))) {
            throw new Exception(String.format("Expected %s key to have a value of type '%s'. "
                            + "Instead it contained a value of type '%s'.",
                    key, expectedClass.toString(), obj.getClass().toString()));
        }
        return (T) obj;
    }

    /**
     * Function to get SchemaConfig with given connection properties.
     *
     * @param gremlinConnectionProperties Connection properties.
     * @return SchemaConfig Object.
     * @throws SQLException if getting the config fails.
     */
    public static SchemaConfig getSchemaConfig(final GremlinConnectionProperties gremlinConnectionProperties)
            throws SQLException {
        final SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setTables(getTableConfigs(MetadataCache.getNodeSchemaList()));
        schemaConfig.setRelationships(
                getTableRelationships(MetadataCache.getEdgeSchemaList(), gremlinConnectionProperties));
        return schemaConfig;
    }

    static List<TableConfig> getTableConfigs(final List<GraphSchema> nodeSchema) {
        // Get Node table information.
        final List<TableConfig> tableConfigList = new ArrayList<>();
        for (final GraphSchema graphSchema : nodeSchema) {
            final TableConfig tableConfig = new TableConfig();
            final List<TableColumn> tableColumns = new ArrayList<>();
            for (final Map<String, Object> properties : graphSchema.getProperties()) {
                final TableColumn tableColumn = new TableColumn();
                tableColumn.setType(properties.get("dataType").toString().toLowerCase());
                tableColumn.setName((String) properties.get("property"));
                tableColumn.setPropertyName(null);
                tableColumns.add(tableColumn);
            }

            // might be the issue
            tableConfig.setName(String.join(":", graphSchema.getLabels()));
            tableConfig.setColumns(tableColumns);
            tableConfigList.add(tableConfig);
        }
        return tableConfigList;
    }

    static List<TableRelationship> getTableRelationships(final List<GraphSchema> edgeSchema,
                                                         final GremlinConnectionProperties gremlinConnectionProperties)
            throws SQLException {
        final List<TableRelationship> tableRelationshipList = new ArrayList<>();
        for (final GraphSchema graphSchema : edgeSchema) {
            final InOutQueries inOutQueries = getInVertex(graphSchema.getLabels());
            final InOutLabels inOutLabels = runInOutQueries(gremlinConnectionProperties, inOutQueries);

            final TableRelationship tableRelationship = new TableRelationship();

            // Handle columns
            final List<TableColumn> tableColumns = new ArrayList<>();
            for (final Map<String, Object> properties : graphSchema.getProperties()) {
                final TableColumn tableColumn = new TableColumn();
                tableColumn.setType(properties.get("dataType").toString().toLowerCase());
                tableColumn.setName((String) properties.get("property"));
                tableColumn.setPropertyName(null);
                tableColumns.add(tableColumn);
            }
            tableRelationship.setColumns(tableColumns);

            // TODO: Investigate how to support this / how this is returned.
            // It is currently unclear if a given edge will return multiple labels if
            //  a) it is connected to vertexes that have different single labels
            //  b) it is connected to a vertex that has multiple labels
            //  c) both of the above
            // Need to figure this out and also do some handling around this. Perhaps we can leverage the vertex schema
            // that the neptune export tool gives us. For now hardcode and assume/handle the simplest case of a single
            // vertex.
            if (inOutLabels.getIn().size() > 1) {
                LOGGER.warn("Multiple in vertex labels not currently supported here, only first is used.");
            }
            if (inOutLabels.getOut().size() > 1) {
                LOGGER.warn("Multiple out vertex labels not currently supported here, only first is used.");
            }
            // TODO: Revisit this null value later.
            // If FkTable is null, it defaults to OutTable.
            tableRelationship.setFkTable(null);
            tableRelationship.setInTable(inOutLabels.getIn().get(0));
            tableRelationship.setOutTable(inOutLabels.getOut().get(0));
            tableRelationship.setEdgeLabel(String.join(":", graphSchema.getLabels()));
            tableRelationshipList.add(tableRelationship);
        }
        return tableRelationshipList;
    }

    static InOutQueries getInVertex(final List<String> labels) {
        // Combine labels into list of comma delimited labels.
        // ["foo", "bar", "baz"] -> "'foo', 'bar', 'baz'"
        // ["foo"] -> "'foo'"
        final String relationshipLabel = labels.stream().collect(Collectors.joining("','", "'", "'"));
        return new InOutQueries(getVertexLabelQuery("in", relationshipLabel),
                getVertexLabelQuery("out", relationshipLabel));
    }

    static String getVertexLabelQuery(final String direction, final String relationshipLabel) {
        // g.V().in(<labels with comma delimits and single quotes>).label().dedup()
        return String.format("g.V().%s(%s).label().dedup()", direction, relationshipLabel);
    }

    static InOutLabels runInOutQueries(final GremlinConnectionProperties gremlinConnectionProperties,
                                       final InOutQueries inOutQueries)
            throws SQLException {
        try {
            final Cluster cluster = GremlinQueryExecutor.createClusterBuilder(gremlinConnectionProperties).create();
            final Client client = cluster.connect().init();

            // Run queries async so they run in parallel.
            final CompletableFuture<ResultSet> inResult = client.submitAsync(inOutQueries.getIn());
            final CompletableFuture<ResultSet> outResult =
                    client.submitAsync(inOutQueries.getOut());

            // Block on in result and transform it to a List of Strings.
            final List<String> inLabels = inResult.get().stream().map(Result::getString).collect(Collectors.toList());

            // Block on out result and transform it to a List of Strings.
            final List<String> outLabels = outResult.get().stream().map(Result::getString).collect(Collectors.toList());
            return new InOutLabels(inLabels, outLabels);
        } catch (final Exception e) {
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.QUERY_FAILED, e);
        }
    }

    @AllArgsConstructor
    @Getter
    private static class InOutQueries {
        private final String in;
        private final String out;
    }

    @AllArgsConstructor
    @Getter
    private static class InOutLabels {
        private final List<String> in;
        private final List<String> out;
    }
}
