package software.amazon.neptune.gremlin.sql;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.SigV4WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.twilmes.sql.gremlin.SqlToGremlin;
import org.twilmes.sql.gremlin.processor.SingleQueryExecutor;
import java.sql.SQLException;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

// Temporary test file to do ad hoc testing.
public class SqlGremlinTest {

    private static final String ENDPOINT = "iam-auth-test-lyndon.cluster-cdubgfjknn5r.us-east-1.neptune.amazonaws.com";
    private static final int PORT = 8182;

    GraphTraversalSource getGraphTraversalSource() {
        final Cluster.Builder builder = Cluster.build();
        builder.addContactPoint(ENDPOINT);
        builder.port(PORT);
        builder.enableSsl(true);
        builder.channelizer(SigV4WebSocketChannelizer.class);

        final Cluster cluster = builder.create();
        final Client client = cluster.connect().init();
        return traversal().withRemote(DriverRemoteConnection.using(client));
    }

    @Test
    @Disabled
    void test() throws SQLException {
        runQueryPrintResults("SELECT * FROM Person");
        runQueryPrintResults("SELECT cat FROM Person");
    }

    void runQueryPrintResults(final String query) throws SQLException {
        final SqlToGremlin sqlToGremlin = new SqlToGremlin(null, getGraphTraversalSource());
        System.out.println("Executing query: " + query);
        System.out.println("Explain: " + sqlToGremlin.explain(query));
        final SingleQueryExecutor.SqlGremlinQueryResult queryResult = sqlToGremlin.execute(query);
        final List<String> columns = queryResult.getColumns();
        final List<Object> rows = queryResult.getRows();
        System.out.println(rows.toString());
        for (final Object obj : rows) {
            if (obj != null) {
                if (obj instanceof Object[]) {
                    final Object[] object = (Object[]) obj;
                    for (final Object o : object) {
                        System.out.println("sub obj: " + ((o == null) ? null : o.toString()));
                    }
                } else {
                    System.out.println("obj: " + obj.toString());
                }
            } else {
                System.out.println("obj: " + null);
            }
        }
    }
}
