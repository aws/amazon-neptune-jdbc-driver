package tdvt;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.joda.time.DateTime;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.neptune.gremlin.sql.SqlGremlinConnection;
import software.aws.neptune.gremlin.sql.SqlGremlinQueryExecutor;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.CONTACT_POINT_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.ENABLE_SSL_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.PORT_KEY;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_HOSTNAME;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_PRIVATE_KEY_FILE;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_STRICT_HOST_KEY_CHECKING;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_USER;

public class TDVTDataUpload {
    private static final String ENDPOINT = "database-1.cluster-cdffsmv2nzf7.us-east-2.neptune.amazonaws.com";
    private static final String SAMPLE_QUERY = "g.V().count()";
    private static final int PORT = 8182;
    private static final int COUNT_PER = 20;
    private static final Dataset DATASET = Dataset.Calcs;
    private static int count = 0;
    private static java.sql.Connection connection;
    private static java.sql.DatabaseMetaData databaseMetaData;
    private static Client client;

    @BeforeAll
    static void initialize() throws SQLException {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set default to IAMSigV4
        properties.put(CONTACT_POINT_KEY, ENDPOINT);
        properties.put(PORT_KEY, PORT);
        properties.put(ENABLE_SSL_KEY, true);
        properties.put(SSH_USER, "ec2-user");
        properties.put(SSH_HOSTNAME, "52.14.185.245");
        properties.put(SSH_PRIVATE_KEY_FILE, "~/Downloads/EC2/neptune-test.pem");
        properties.put(SSH_STRICT_HOST_KEY_CHECKING, "false");
        final GremlinConnectionProperties gremlinConnectionProperties = new GremlinConnectionProperties(properties);

        final java.sql.Connection tempConnection = new SqlGremlinConnection(gremlinConnectionProperties);

        final Cluster cluster = SqlGremlinQueryExecutor.createClusterBuilder(gremlinConnectionProperties).create();
        client = cluster.connect().init();
        final ResultSet results = client.submit("inject(0)");
        System.out.println(results.toString());
    }

    @AfterEach
    void deinitialize() throws SQLException {
        if (client != null) {
            client.close();
        }
    }

    Object attemptDateConversion(final Object data) {
        try {
            // return Date.parse(data.toString());
            //final DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
            // final LocalDateTime localDateTime = LocalDateTime.parse(data.toString(), formatter);
            //System.out.println("Formatted as date.");
            Date date = DateTime.parse(data.toString()).toDate();
            System.out.println("Input: " + data.toString() + " - Output: " + date.toString());
            return date;
        } catch (final Exception ignored) {
            System.out.println("Failed to convert " + data.toString());
            return data;
        }
    }

    GraphTraversal<?, ?> appendTraversal(final BufferedReader br, final GraphTraversalSource g)
            throws Exception {
        count = 0;
        GraphTraversal<?, ?> graphTraversal = null;
        for (String line; ((line = br.readLine()) != null && (count < COUNT_PER)); ) {
            if (line.trim().length() == 0) {
                continue;
            }
            graphTraversal = (graphTraversal == null) ? (DATASET == Dataset.Calcs ? g.addV("Calcs") : g.addV("Staples"))
                    : (DATASET == Dataset.Calcs ? graphTraversal.addV("Calcs") : graphTraversal.addV("Staples"));
            count++;
            final JSONParser parser = new JSONParser();
            final JSONObject json = (JSONObject) parser.parse(line);
            for (final Object key : json.keySet()) {
                Object value = json.get(key);
                if (value == null) {
                    continue;
                }
                if (value instanceof String) {
                    value = StringEscapeUtils.unescapeJava(value.toString());
                    value = attemptDateConversion(value);
                } else if (value instanceof Boolean) {
                    System.out.println("Boolean! " + value);
                }
                graphTraversal.property(key, value);
            }
        }
        return graphTraversal;
    }

    @Test
    void loadData() throws SQLException, ExecutionException, InterruptedException {
        GraphTraversal<?, ?> graphTraversal = null;
        try {
            final String fileName = "/Users/lyndonb/Desktop/calcs_gremlin.json";
            if (DATASET.equals(Dataset.Calcs)) {
                if (!fileName.toLowerCase().contains("calcs")) {
                    throw new Exception("Possible error in data upload.");
                }
                deleteTable("Calcs");
            } else if (DATASET.equals(Dataset.Staples)) {
                if (!fileName.toLowerCase().contains("staples")) {
                    throw new Exception("Possible error in data upload.");
                }
                deleteTable("Staples");
            }
            final File file = new File(fileName);
            int total = 0;
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(client));
            final BufferedReader br = new BufferedReader(new FileReader(file));
            do {
                graphTraversal = appendTraversal(br, g);
                graphTraversal.iterate();
                total += count;
                System.out.println("Executing " + count + " queries up to " + total);
            } while (count == COUNT_PER);
            System.out.println("Total queries " + total);
        } catch (final Exception e) {
            e.printStackTrace();
            if (graphTraversal != null) {
                System.out.println(
                        "Traversal: " + GroovyTranslator.of("g").translate(graphTraversal.asAdmin().getBytecode()));
            }
            Assert.fail(e.getMessage());
        }
    }

    void deleteTable(final String table) {
        long vertexCount = client.submit("g.V().hasLabel('" + table + "').count()").one().getLong();
        while (vertexCount > 0) {
            final Result result = client.submit("g.V().hasLabel('" + table + "').limit(5000).drop().iterate()").one();
            vertexCount -= 5000;
            System.out.println("Dropped 5000, " + vertexCount + " left.");
        }
    }

    @Test
    void getVertexCount() {
        long vertexCount = client.submit("g.V().hasLabel('Staples').count()").one().getLong();
        System.out.println("Vertex Staples: " + vertexCount);
        vertexCount = client.submit("g.V().hasLabel('staples').count()").one().getLong();
        System.out.println("Vertex staples: " + vertexCount);
        vertexCount = client.submit("g.V().hasLabel('Calcs').count()").one().getLong();
        System.out.println("Vertex Calcs: " + vertexCount);
        vertexCount = client.submit("g.V().hasLabel('calcs').count()").one().getLong();
        System.out.println("Vertex calcs: " + vertexCount);
    }

    enum Dataset {
        Calcs,
        Staples
    }


}
