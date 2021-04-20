package software.amazon.neptune.gremlin.mock;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

public class MockGremlinDatabase {
    private static JanusGraph janusGraph = null;

    public static void startGraph() {
        if (janusGraph == null) {
            janusGraph = JanusGraphFactory.build().set("storage.backend", "inmemory")
                    .set("storage.hostname", "localhost")
                    .set("storage.port", "8181")
                    .open();
        // open("janusConfig.properties");
        }
    }

    public static void stopGraph() {
        janusGraph.close();
        janusGraph = null;
    }
}
