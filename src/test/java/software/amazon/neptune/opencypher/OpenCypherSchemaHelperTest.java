/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License testIs located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file testIs distributed
 * on an "AS testIs" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.neptune.opencypher;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.neptune.opencypher.resultset.OpenCypherResultSetGetColumns;
import software.amazon.neptune.opencypher.utilities.OpenCypherGetColumnUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class OpenCypherSchemaHelperTest {

    @Test
    void deleteDirectoryIfExistsTest() throws IOException {
        final Path root = OpenCypherGetColumnUtilities.getAndClearUniqueDirectoryForThread();
        OpenCypherGetColumnUtilities.createFiles(root, ImmutableList.of("testFile.json"));

        Assertions.assertTrue(root.toAbsolutePath().toFile().isDirectory());
        OpenCypherSchemaHelper.deleteDirectoryIfExists(root);
        Assertions.assertFalse(root.toAbsolutePath().toFile().isDirectory());
    }

    @Test
    void getOutputFilesTest() throws Exception {
        final Path root = OpenCypherGetColumnUtilities.getAndClearUniqueDirectoryForThread();
        final Path extendedRoot = new File(root.toAbsolutePath().toString() + "/extendedDirectory").toPath();

        final List<String> testFiles = ImmutableList.of(
                "testFile.json",
                "testFile2.json"
        );
        final List<String> testFilesFullPath = new ArrayList<>();
        testFiles.forEach(tf -> testFilesFullPath.add(extendedRoot.toAbsolutePath().toString() + "/" + tf));
        OpenCypherGetColumnUtilities.createFiles(extendedRoot, testFiles);

        final List<String> outputFiles = OpenCypherSchemaHelper.getOutputFiles(root.toAbsolutePath().toString());
        Assertions.assertEquals(new HashSet<>(testFilesFullPath.stream().map(p -> p.replace("/", "\\")).collect(
                Collectors.toSet())), new HashSet<>(outputFiles.stream().map(p -> p.replace("/", "\\")).collect(
                Collectors.toSet())));
    }

    @Test
    void parseOutputFilesTest() throws Exception {
        final Path root = OpenCypherGetColumnUtilities.getAndClearUniqueDirectoryForThread();
        final Path extendedRoot = new File(root.toAbsolutePath().toString() + "/extendedDirectory").toPath();

        final List<String> testFiles = ImmutableList.of(
                "testFile.json"
        );
        final List<String> testFilesFullPath = new ArrayList<>();
        testFiles.forEach(tf -> testFilesFullPath.add(extendedRoot.toAbsolutePath().toString() + "/" + tf));
        OpenCypherGetColumnUtilities.createFiles(extendedRoot, testFiles);

        final List<OpenCypherResultSetGetColumns.NodeColumnInfo> nodeColumnInfoList = new ArrayList<>();
        testFilesFullPath
                .forEach(testFileFullPath -> OpenCypherSchemaHelper.parseFile(testFileFullPath, nodeColumnInfoList));
        Assertions.assertEquals(nodeColumnInfoList.size(), OpenCypherGetColumnUtilities.NODE_COLUMN_INFOS.size());
        for (final OpenCypherResultSetGetColumns.NodeColumnInfo nodeColumnInfo : nodeColumnInfoList) {
            Assertions.assertTrue(OpenCypherGetColumnUtilities.NODE_COLUMN_INFOS.contains(nodeColumnInfo));
        }
    }
}
