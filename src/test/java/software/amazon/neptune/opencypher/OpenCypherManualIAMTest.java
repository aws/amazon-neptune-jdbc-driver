package software.amazon.neptune.opencypher;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import software.amazon.jdbc.utilities.ConnectionProperties;
import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class OpenCypherManualIAMTest {

    private static final String HOSTNAME = "iam-auth-test.cluster-cdubgfjknn5r.us-east-1.neptune.amazonaws.com";

    @Disabled
    @Test
    void testBasicIamAuthDirectly() throws Exception {
        final String endpoint = String.format("bolt://%s:%d", HOSTNAME, 8182);
        final Config config = Config.builder()
                .withConnectionTimeout(3, TimeUnit.SECONDS)
                .withMaxConnectionPoolSize(1000)
                .withEncryption()
                .withTrustStrategy(Config.TrustStrategy.trustAllCertificates())
                .build();
        final Driver driver = GraphDatabase
                .driver(endpoint, OpenCypherIAMRequestGenerator.getSignedHeader(endpoint, "us-east-1"), config);
        // This will throw and fail if authentication fails.
        driver.verifyConnectivity();
    }

    @Disabled
    @Test
    void testBasicIamAuthJDBC() throws Exception {
        final String endpoint = String.format("bolt://%s:%d", HOSTNAME, 8182);
        final String region = "us-east-1";
        final String auth = "IamSigV4";
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.ENDPOINT_KEY, endpoint);
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, auth);
        properties.put(ConnectionProperties.REGION_KEY, region);
        final Connection connection = new OpenCypherConnection(new ConnectionProperties(properties));
        Assertions.assertTrue(connection.isValid(1000));
    }
}
