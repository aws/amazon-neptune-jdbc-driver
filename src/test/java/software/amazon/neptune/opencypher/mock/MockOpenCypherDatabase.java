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

import org.junit.ClassRule;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Settings;
import java.io.File;
import java.util.Objects;
import static org.neo4j.helpers.ListenSocketAddress.listenAddress;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.TRUE;

// TODO: AN-354 Abstract logic of this to create a nice interface to easily spin up different datasets.
public class MockOpenCypherDatabase {
    private static final File DB_PATH = new File("target/neo4j-test");
    private final GraphDatabaseService graphDb;

    @ClassRule
    private static final Neo4jRule NEO4J_RULE = new Neo4jRule();

    /**
     * OpenCypherDatabase constructor.
     * @param host Host to initialize with.
     * @param port Port to initialize with.
     */
    public MockOpenCypherDatabase(final String host, final int port) {
        if (DB_PATH.exists()) {
            for (final String fileName : Objects.requireNonNull(DB_PATH.list())) {
                final File file = new File(DB_PATH, fileName);
                file.delete();
            }
        }
        final BoltConnector boltConnector = new BoltConnector("bolt");
        graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(DB_PATH)
                .setConfig(Settings.setting("dbms.directories.import", STRING, "data"),"../../data")
                .setConfig(boltConnector.type, BOLT.name())
                .setConfig(boltConnector.enabled, TRUE)
                .setConfig(boltConnector.listen_address, listenAddress(host, port))
                .setConfig(boltConnector.encryption_level, DISABLED.name())
                .setConfig(GraphDatabaseSettings.auth_enabled, FALSE)
                .newGraphDatabase();
        graphDb.execute("CREATE INDEX ON :Person(first_name, last_name)");
        graphDb.execute("CREATE INDEX ON :Cat(name)");
        graphDb.execute("CREATE CONSTRAINT ON (p:Person) ASSERT p.first_name IS UNIQUE");
        graphDb.execute("CREATE (l:Person {first_name:'lyndon', last_name:'bauto'}) " +
                "CREATE (v:Person {first_name:'valentina', last_name:'bozanovic'}) " +
                "CREATE (c_l:Cat {name:'vinny'}) " +
                "CREATE (c_v:Cat {name:'tootsie'}) " +
                "CREATE (l)-[rlv:KNOWS]->(v)" +
                "CREATE (v)-[rvl:KNOWS]->(l)" +
                "CREATE (l)-[rlcl:GIVES_PETS_TO]->(c_l) " +
                "CREATE (v)-[rvcv:GIVES_PETS_TO]->(c_v) " +
                "CREATE (c_l)-[rcll:GETS_PETS_FROM]->(l) " +
                "CREATE (c_v)-[rcvv:GETS_PETS_FROM]->(v)");
    }

    /**
     * Function to shutdown the database.
     */
    public void shutdown() {
        graphDb.shutdown();
    }
}
