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

package software.amazon.neptune.common.gremlindatamodel;

import com.amazonaws.services.neptune.NeptuneExportCli;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.utilities.SqlError;
import software.amazon.jdbc.utilities.SqlState;
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
                                      final List<GraphSchema> edgeSchemaList)
            throws SQLException, IOException {
        // Create unique directory if doesn't exist
        // If does exist, delete current contents
        final String directory = createUniqueDirectoryForThread();

        // Run process
        final List<String> outputFiles = runGremlinSchemaGrabber(endpoint, nodes, directory, useIAM, pathType);

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
                                                        final MetadataCache.PathType pathType)
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
            List<String> labels;
            try {
                labels = getValueCheckType(node, "label", ArrayList.class);
            } catch (final Exception ignored) {
                labels = ImmutableList.of(getValueCheckType(node, "label", String.class));
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
}