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

package software.amazon.neptune.opencypher.mock;

import lombok.Getter;
import lombok.SneakyThrows;
import org.junit.ClassRule;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Settings;
import java.io.File;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import static org.neo4j.helpers.ListenSocketAddress.listenAddress;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.TRUE;

// TODO: AN-354 Abstract logic of this to create a nice interface to easily spin up different datasets.
public final class MockOpenCypherDatabase {
    private static final String DB_PATH = "target/neo4j-test/";
    @ClassRule
    private static final Neo4jRule NEO4J_RULE = new Neo4jRule();
    private final GraphDatabaseService graphDb;
    @Getter
    private final String host;
    @Getter
    private final int port;

    // Need lock to make sure we don't have port grab collisions (need to wait for binding).
    private static final Object LOCK = new Object();

    /**
     * OpenCypherDatabase constructor.
     *
     * @param host Host to initialize with.
     * @param port Port to initialize with.
     */
    private MockOpenCypherDatabase(final String host, final int port, final String path) {
        this.host = host;
        this.port = port;
        final File dbPath = new File(DB_PATH + path);
        if (dbPath.exists()) {
            for (final String fileName : Objects.requireNonNull(dbPath.list())) {
                final File file = new File(dbPath, fileName);
                file.delete();
            }
        }
        final BoltConnector boltConnector = new BoltConnector("bolt");
        graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(dbPath)
                .setConfig(Settings.setting("dbms.directories.import", STRING, "data"), "../../data")
                .setConfig(boltConnector.type, BOLT.name())
                .setConfig(boltConnector.enabled, TRUE)
                .setConfig(boltConnector.listen_address, listenAddress(host, port))
                .setConfig(boltConnector.encryption_level, DISABLED.name())
                .setConfig(GraphDatabaseSettings.auth_enabled, FALSE)
                .newGraphDatabase();
    }

    /**
     * Function to initiate builder for MockOpenCypherDatabase
     *
     * @param host Host to use.
     * @param callingClass Class calling builder (used for unique path).
     * @return Builder pattern for MockOpenCypherDatabase.
     */
    @SneakyThrows
    public static MockOpenCypherDatabaseBuilder builder(final String host, final String callingClass) {
        synchronized (LOCK) {
            int attempts = 0;
            while (attempts < 10) {
                try {
                    // Get random unassigned port.
                    attempts++;
                    final ServerSocket socket = new ServerSocket(0);
                    final int port = socket.getLocalPort();
                    socket.setReuseAddress(true);
                    socket.close();
                    final MockOpenCypherDatabase db = new MockOpenCypherDatabase(host, port, callingClass);
                    return new MockOpenCypherDatabaseBuilder(db);
                } catch (final Exception ignored) {
                }
            }
        }
        throw new Exception("Unable to find port.");
    }

    /**
     * Function to generate a create node query.
     *
     * @param mockNode Node to create.
     * @return Create node query.
     */
    private static String createNode(final MockOpenCypherNode mockNode) {
        return String.format("CREATE (%s:%s)", mockNode.getAnnotation(), mockNode.getInfo());
    }

    /**
     * Function to generate a create relationship query from (a)-[rel]->(b).
     *
     * @param mockNode1    Node to create relationship from (a).
     * @param mockNode2    Node to create relationship to (b).
     * @param relationship Relationship between notes [rel].
     * @return Create relationship query.
     */
    private static String createRelationship(final MockOpenCypherNode mockNode1, final MockOpenCypherNode mockNode2,
                                             final String relationship) {
        return String
                .format("CREATE (%s)-[%s:%s]->(%s)", mockNode1.getAnnotation(), MockOpenCypherNodes.getNextAnnotation(),
                        relationship, mockNode2.getAnnotation());
    }

    /**
     * Function to create an index query.
     *
     * @param mockNode Node to create index on.
     * @return Create index query.
     */
    private static String createIndex(final MockOpenCypherNode mockNode) {
        return String.format("CREATE INDEX ON :%s", mockNode.getIndex());
    }

    void executeQuery(final String query) {
        graphDb.execute(query);
    }

    /**
     * Function to shutdown the database.
     */
    public void shutdown() {
        graphDb.shutdown();
    }

    public static class MockOpenCypherDatabaseBuilder {
        private final MockOpenCypherDatabase db;
        private final List<String> indexes = new ArrayList<>();
        private final List<String> nodes = new ArrayList<>();
        private final List<String> relationships = new ArrayList<>();

        MockOpenCypherDatabaseBuilder(final MockOpenCypherDatabase db) {
            this.db = db;
        }

        /**
         * Builder pattern node insert function.
         *
         * @param node Node to insert.
         * @return Builder.
         */
        public MockOpenCypherDatabaseBuilder withNode(final MockOpenCypherNode node) {
            nodes.add(createNode(node));
            if (!indexes.contains(createIndex(node))) {
                indexes.add(createIndex(node));
            }
            return this;
        }

        /**
         * Builder pattern relationship insert (a)-[rel]->(b)
         *
         * @param node1        Node (a) to make relationship from.
         * @param node2        Node (b) to make relationship to.
         * @param relationship Relationship [rel] from (a) to (b).
         * @return Builder.
         */
        public MockOpenCypherDatabaseBuilder withRelationship(final MockOpenCypherNode node1,
                                                              final MockOpenCypherNode node2,
                                                              final String relationship) {
            relationships.add(createRelationship(node1, node2, relationship));
            return this;
        }

        /**
         * Builder pattern relationship insert (a)-[rel1]->(b) and (b)-[rel2]->(a)
         *
         * @param node1         Node (a) for relationship.
         * @param node2         Node (b) for relationship.
         * @param relationship1 Relationship [rel1] from (a) to (b).
         * @param relationship2 Relationship [rel2] from (b) to (b).
         * @return Builder.
         */
        public MockOpenCypherDatabaseBuilder withRelationship(final MockOpenCypherNode node1,
                                                              final MockOpenCypherNode node2,
                                                              final String relationship1, final String relationship2) {
            relationships.add(createRelationship(node1, node2, relationship1));
            relationships.add(createRelationship(node2, node1, relationship2));
            return this;
        }

        /**
         * Function to build MockOpenCypherDatabase Object.
         *
         * @return Constructed database.
         */
        public MockOpenCypherDatabase build() {
            if (!indexes.isEmpty()) {
                indexes.forEach(db::executeQuery);
            }
            String query = "";
            if (!nodes.isEmpty()) {
                query = String.join(" ", nodes);
                if (!relationships.isEmpty()) {
                    query += " " + String.join(" ", relationships);
                }
            }
            if (!query.isEmpty()) {
                db.executeQuery(query);
            }
            return db;
        }
    }
}
