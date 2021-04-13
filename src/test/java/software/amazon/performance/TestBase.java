/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.performance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.junit.jupiter.api.Assertions;
import software.amazon.neptune.NeptuneDriver;
import software.amazon.performance.config.TestInfo;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class TestBase {
    private static final String YAML_DIR = "./src/test/java/software/amazon/performance/yaml";
    private static final String[] YAML = new String[] { "yaml" };

    private static List<TestInfo> testConfigList = null;

    private static void loadTestConfigList() {
        testConfigList = new ArrayList<>();

        final List<File> files = (List<File>) FileUtils.listFiles(new File(YAML_DIR), YAML, true);
        for (File file : files) {
            try {
                System.out.println("file: " + file.getCanonicalPath());
                final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                final TestInfo testInfo = mapper.readValue(file, TestInfo.class);
                System.out.println("testInfo: " +
                        ReflectionToStringBuilder.toString(testInfo, ToStringStyle.MULTI_LINE_STYLE));
                testConfigList.add(testInfo);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    protected static List<TestInfo> getTestConfigList() {
        if (Objects.isNull(testConfigList)) {
            loadTestConfigList();
        }
        Assertions.assertFalse(testConfigList.isEmpty());
        return testConfigList;
    }

    protected static java.sql.Connection getConnection(final TestInfo testInfo) throws SQLException {
        final java.sql.Driver driver = new NeptuneDriver();
        final java.sql.Connection connection = driver.connect(testInfo.url(), new Properties());
        Assertions.assertNotNull(connection);
        Assertions.assertTrue(connection.isValid(100));

        return connection;
    }
}
