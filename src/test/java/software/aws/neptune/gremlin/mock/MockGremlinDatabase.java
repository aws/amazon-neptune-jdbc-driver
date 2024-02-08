/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.gremlin.mock;

import org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.io.Storage;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MockGremlinDatabase {

    private static GremlinServer server;

    /**
     * Starts a new instance of Gremlin Server.
     */
    public static void startServer() throws Exception {
        final InputStream stream = MockGremlinDatabase.class.getResourceAsStream("/gremlin-server.yaml");
        final Settings settings = Settings.read(stream);
        rewritePathsInGremlinServerSettings(settings);
        server = new GremlinServer(settings);
        server.start().join();
    }

    /**
     * Stops a current instance of Gremlin Server.
     */
    public static void stopServer() {
        server.stop().join();
        server = null;
    }

    /**
     *  Tests from maven will pass this value in via failsafe plugin. basically we want to home this in on the
     *  gremlin-server directory to get stuff to reference scripts properly from the file system from any directory.
     *  If an overriden path is determined to be absolute then the path is not re-written.
     */
    private static void rewritePathsInGremlinServerSettings(final Settings overridenSettings) {
        final Map<String, Map<String, Object>> plugins;
        final Map<String, Object> scriptFileGremlinPlugin;
        final File homeDir;

        homeDir = new File(Paths.get("./src/test/scripts").toAbsolutePath().toString());

        plugins = overridenSettings.scriptEngines.get("gremlin-groovy").plugins;
        scriptFileGremlinPlugin = plugins.get(ScriptFileGremlinPlugin.class.getName());

        if (scriptFileGremlinPlugin != null) {
            scriptFileGremlinPlugin
                    .put("files",
                            ((List<String>) scriptFileGremlinPlugin.get("files")).stream()
                                    .map(s -> new File(s))
                                    .map(f -> f.isAbsolute() ? f
                                            : new File(relocateFile( homeDir, f)))
                                    .map(f -> Storage.toPath(f))
                                    .collect(Collectors.toList()));
        }

        overridenSettings.graphs = overridenSettings.graphs.entrySet().stream()
                .map(kv -> {
                    kv.setValue(relocateFile( homeDir, new File(kv.getValue())));
                    return kv;
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    }

    /**
     * Strip the directories from the configured filePath, replace them with homeDir, this way relocating
     * the files
     *
     * @param homeDir
     * @param filePath absolute or relative file path
     * @return the absolute storage path of the relocated file
     */
    private static String relocateFile(final File homeDir, File filePath) {
        final String plainFileName = filePath.getName();

        return Storage.toPath(new File(homeDir, plainFileName));
    }

}
