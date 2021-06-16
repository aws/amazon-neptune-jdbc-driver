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

package software.amazon.neptune.opencypher.utilities;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import software.amazon.neptune.common.gremlindatamodel.GraphSchema;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class OpenCypherGetColumnUtilities {
    public static final List<String> COLUMN_NAMES = new ArrayList<>();
    public static final String EXAMPLE_OUTPUT = "{\n" +
            "  \"nodes\" : [ {\n" +
            "    \"label\" : [ \"A\", \"B\", \"C\" ],\n" +
            "    \"properties\" : [ {\n" +
            "      \"property\" : \"name\",\n" +
            "      \"dataType\" : \"String\",\n" +
            "      \"isMultiValue\" : false,\n" +
            "      \"isNullable\" : false\n" +
            "    }, {\n" +
            "      \"property\" : \"email\",\n" +
            "      \"dataType\" : \"String\",\n" +
            "      \"isMultiValue\" : false,\n" +
            "      \"isNullable\" : false\n" +
            "    } ]\n" +
            "  }, {\n" +
            "    \"label\" : [ \"A\", \"B\", \"C\", \"D\" ],\n" +
            "    \"properties\" : [ {\n" +
            "      \"property\" : \"age\",\n" +
            "      \"dataType\" : \"Integer\",\n" +
            "      \"isMultiValue\" : false,\n" +
            "      \"isNullable\" : false\n" +
            "    }, {\n" +
            "      \"property\" : \"email\",\n" +
            "      \"dataType\" : \"String\",\n" +
            "      \"isMultiValue\" : false,\n" +
            "      \"isNullable\" : false\n" +
            "    } ]\n" +
            "  } ]\n" +
            "}";
    public static final List<GraphSchema> NODE_COLUMN_INFOS = ImmutableList.of(
            new GraphSchema(
                    ImmutableList.of("A", "B", "C"),
                    ImmutableList.of(
                            ImmutableMap.of(
                                    "property", "name",
                                    "dataType", "String",
                                    "isMultiValue", false,
                                    "isNullable", false),
                            ImmutableMap.of(
                                    "property", "email",
                                    "dataType", "String",
                                    "isMultiValue", false,
                                    "isNullable", false)
                    )
            ),
            new GraphSchema(
                    ImmutableList.of("A", "B", "C", "D"),
                    ImmutableList.of(
                            ImmutableMap.of(
                                    "property", "age",
                                    "dataType", "Integer",
                                    "isMultiValue", false,
                                    "isNullable", false),
                            ImmutableMap.of(
                                    "property", "email",
                                    "dataType", "String",
                                    "isMultiValue", false,
                                    "isNullable", false)
                    )
            )
    );

    static {
        COLUMN_NAMES.add("TABLE_CAT");
        COLUMN_NAMES.add("TABLE_SCHEM");
        COLUMN_NAMES.add("TABLE_NAME");
        COLUMN_NAMES.add("COLUMN_NAME");
        COLUMN_NAMES.add("DATA_TYPE");
        COLUMN_NAMES.add("TYPE_NAME");
        COLUMN_NAMES.add("COLUMN_SIZE");
        COLUMN_NAMES.add("BUFFER_LENGTH");
        COLUMN_NAMES.add("DECIMAL_DIGITS");
        COLUMN_NAMES.add("NUM_PREC_RADIX");
        COLUMN_NAMES.add("NULLABLE");
        COLUMN_NAMES.add("REMARKS");
        COLUMN_NAMES.add("COLUMN_DEF");
        COLUMN_NAMES.add("SQL_DATA_TYPE");
        COLUMN_NAMES.add("SQL_DATETIME_SUB");
        COLUMN_NAMES.add("CHAR_OCTET_LENGTH");
        COLUMN_NAMES.add("ORDINAL_POSITION");
        COLUMN_NAMES.add("IS_NULLABLE");
        COLUMN_NAMES.add("SCOPE_CATALOG");
        COLUMN_NAMES.add("SCOPE_SCHEMA");
        COLUMN_NAMES.add("SCOPE_TABLE");
        COLUMN_NAMES.add("SOURCE_DATA_TYPE");
        COLUMN_NAMES.add("IS_AUTOINCREMENT");
        COLUMN_NAMES.add("IS_GENERATEDCOLUMN");
    }

    /**
     * Function to create files named after list at path. Content of file is example schema.
     *
     * @param path      Path to create files at (root).
     * @param fileNames Names of files to create.
     * @throws IOException Thrown if file creation fails.
     */
    public static void createFiles(final Path path, final List<String> fileNames) throws IOException {
        final File outputDirectory = new File(path.toAbsolutePath().toString());
        Assertions.assertTrue(outputDirectory.mkdirs());
        for (final String fileName : fileNames) {
            final File outputFile = new File(outputDirectory.getAbsolutePath() + "/" + fileName);
            Assertions.assertTrue(outputFile.createNewFile());
            final FileWriter fileWriter = new FileWriter(outputFile);
            fileWriter.write(EXAMPLE_OUTPUT);
            fileWriter.close();
        }
    }

    /**
     * Gets directory path that is unique to the thread (does not create directory).
     *
     * @return Path to the unique directory.
     */
    public static Path getUniqueDirectoryForThread() {
        return Paths.get(String.format("%d", Thread.currentThread().getId())).toAbsolutePath();
    }

    /**
     * Gets a unique path and makes sure it is cleared out.
     *
     * @return Path to unique directory.
     * @throws IOException thrown if file deletion fails.
     */
    public static Path getAndClearUniqueDirectoryForThread() throws IOException {
        final Path root = getUniqueDirectoryForThread();
        if (root.toFile().isDirectory()) {
            Files.walk(root)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        return root;
    }
}
