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

package software.aws.neptune.gremlin.sql;

import com.github.javafaker.Address;
import com.github.javafaker.Cat;
import com.github.javafaker.Commerce;
import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import com.google.common.collect.ImmutableList;
import dnl.utils.text.table.TextTable;
import javafx.util.Pair;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.SigV4WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.neptune.gremlin.GremlinConnection;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.CONTACT_POINT_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.ENABLE_SSL_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.PORT_KEY;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_HOSTNAME;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_PRIVATE_KEY_FILE;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_STRICT_HOST_KEY_CHECKING;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_USER;

// Temporary test file to do ad hoc testing.
public class SqlGremlinTest {
    private static final String ENDPOINT = "database-1.cluster-cdffsmv2nzf7.us-east-2.neptune.amazonaws.com";
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
    void load() throws SQLException {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set default to IAMSigV4
        properties.put(CONTACT_POINT_KEY, ENDPOINT);
        properties.put(PORT_KEY, PORT);
        properties.put(ENABLE_SSL_KEY, true);

        final java.sql.Connection connection = new GremlinConnection(new GremlinConnectionProperties(properties));

        final Faker faker = new Faker();
        for (int i = 0; i < 10000; i++) {
            System.out.println("Executing query " + (i + 1) + " / 10000.");
            connection.createStatement()
                    .executeQuery(addV(faker.address(), faker.cat(), faker.name(), faker.commerce()));
        }
    }

    String addV(final Address address, final Cat cat, final Name name, final Commerce commerce) {
        final String stringBuilder = "g" +
                // Generate vertexes
                String.format(".addV('%s')", "Address") +
                String.format(".property('streetAddress', '%s')", address.streetAddress().replace("'", "")) +
                String.format(".property('buildingNumber', '%s')", address.buildingNumber().replace("'", "")) +
                String.format(".property('cityName', '%s')", address.cityName().replace("'", "")) +
                String.format(".property('state', '%s')", address.state().replace("'", "")) +
                ".as('addr')" +
                String.format(".addV('%s')", "Cat") +
                String.format(".property('name', '%s')", cat.name().replace("'", "")) +
                String.format(".property('breed', '%s')", cat.breed().replace("'", "")) +
                ".as('c')" +
                String.format(".addV('%s')", "Person") +
                String.format(".property('firstName', '%s')", name.firstName().replace("'", "")) +
                String.format(".property('lastName', '%s')", name.lastName().replace("'", "")) +
                String.format(".property('title', '%s')", name.title().replace("'", "")) +
                ".as('p')" +
                String.format(".addV('%s'n)", "Commerce") +
                String.format(".property('color', '%s')", commerce.color().replace("'", "")) +
                String.format(".property('department', '%s')", commerce.department().replace("'", "")) +
                String.format(".property('material', '%s')", commerce.material().replace("'", "")) +
                String.format(".property('price', '%s')", commerce.price().replace("'", "")) +
                String.format(".property('productName', '%s')", commerce.productName().replace("'", "")) +
                String.format(".property('promotionCode', '%s')", commerce.promotionCode().replace("'", "")) +
                ".as('comm')";
        return stringBuilder;
    }

    @Test
    void testSql() throws SQLException {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set default to IAMSigV4
        properties.put(CONTACT_POINT_KEY, ENDPOINT);
        properties.put(PORT_KEY, PORT);
        properties.put(ENABLE_SSL_KEY, true);
        properties.put(SSH_USER, "ec2-user");
        properties.put(SSH_HOSTNAME, "52.14.185.245");
        properties.put(SSH_PRIVATE_KEY_FILE, "~/Downloads/EC2/neptune-test.pem");
        properties.put(SSH_STRICT_HOST_KEY_CHECKING, "false");


        final List<String> queries = ImmutableList.of(
                "SELECT \n" +
                        "     `airport`.`CONTAINS_ID` AS `CONTAINS_ID`, \n" +
                        "     `airport`.`ROUTE_ID` AS `ROUTE_ID`, \n" +
                        "     `airport`.`city` AS `city`,\n" +
                        "     `airport`.`code` AS `code`,\n" +
                        "     `airport`.`country` AS `country`,\n" +
                        "     `airport`.`desc` AS `desc`,\n" +
                        "     `airport`.`elev` AS `elev`,\n" +
                        "     `airport`.`icao` AS `icao`,\n" +
                        "     `airport`.`lat` AS `lat`,\n" +
                        "     `airport`.`lon` AS `lon`,\n" +
                        "     `airport`.`longest` AS `longest`,\n" +
                        "     `airport`.`region` AS `region`,\n" +
                        "     `airport`.`runways` AS `runways`,\n" +
                        "     `airport`.`type` AS `type`\n" +
                        "   FROM `gremlin`.`airport` `airport`\n" +
                        "   LIMIT 100",
                "SELECT " +
                        " AVG(`airport`.`lat`) AS `avg`," +
                        " `airport`.`lat` AS `lat`" +
                        " FROM `gremlin`.`airport` `airport`" +
                        " GROUP BY `airport`.`lat`",

                "SELECT " +
                        "                     `airport1`.`ROUTE_ID` as `arid1`, " +
                        "                     `airport`.`ROUTE_ID` as `arid0`, " +
                        "                     `airport1`.`city` AS `city__airport__`, " +
                        "                     `airport`.`city` AS `city`, " +
                        "                     AVG(`airport`.`lat`) AS `avg`" +
                        "                           FROM `gremlin`.`airport` `airport` " +
                        "                               INNER JOIN `gremlin`.`airport` `airport1` " +
                        "                                   ON (`airport`.`ROUTE_ID` = `airport1`.`ROUTE_ID`) " +
                        "                                       GROUP BY `airport`.`lat`, `airport1`.`city`, `airport`.`city`," +
                        "                                                 `airport1`.`ROUTE_ID`, `airport`.`ROUTE_ID`" +
                        "   LIMIT 100",
                "SELECT \"route\".\"dist\" AS \"dist\" FROM \"gremlin\".\"route\" \"route\" LIMIT 10000"

                // "SELECT ROUTE_ID, ROUTE_ID as rid, COUNT(ROUTE_ID) AS cnt, AVG(ROUTE_ID) AS a, SUM(SQRT(ROUTE_ID)) AS s FROM airport " +
                //         "GROUP BY ROUTE_ID",

                //"SELECT ROUTE_ID as r FROM airport as a LIMIT 100");
        );
        final java.sql.Connection connection = new SqlGremlinConnection(new GremlinConnectionProperties(properties));
        for (final String query : queries) {
            runQueryPrintResults(query, connection.createStatement());
        }
    }

    @Test
    @Disabled
    void testSqlConnectionExecution() throws SQLException {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set default to IAMSigV4
        properties.put(CONTACT_POINT_KEY, ENDPOINT);
        properties.put(PORT_KEY, PORT);
        properties.put(ENABLE_SSL_KEY, true);

        final List<String> queries = ImmutableList.of("SELECT * FROM Person",
                "SELECT `Person`.`firstName` AS `firstName`, `Cat`.`name` AS `name` FROM `Cat` INNER JOIN `Person` ON (`Cat`.`name` = `Person`.`name`) GROUP BY `Person`.`firstName`, `Cat`.`name`",
                "SELECT `Person`.`age` AS `age`, `Cat`.`name` AS `name__Cat_` FROM `Person` INNER JOIN `Cat` ON (`Person`.`firstName` = `Cat`.`name`) GROUP BY `Person`.`age`, `Cat`.`name`",
                "SELECT `Person`.`firstName` AS `firstName`, `Cat`.`name` AS `name` FROM `Cat` INNER JOIN `Person` ON `Cat`.`name` = `Person`.`name`",
                "SELECT `Person`.`age` AS `age`, SUM(1) AS `cnt_Person_4A9569D21233471BB4DC6258F15087AD_ok`, `Person`.`pets` AS `pets` FROM `Person` GROUP BY `Person`.`age`, `Person`.`pets`",
                "SELECT `Person`.`age` AS `age`, `Person`.`cat` AS `cat`, `Person`.`dog` AS `dog`, `Person`.`firstName` AS `firstName`, `Person`.`lastName` AS `lastName`, `Person`.`name` AS `name`, `Person`.`pets` AS `pets`, `Person`.`title` AS `title` FROM `Person` LIMIT 10000");

        final java.sql.Connection connection = new SqlGremlinConnection(new GremlinConnectionProperties(properties));
        final java.sql.DatabaseMetaData databaseMetaData = connection.getMetaData();
        databaseMetaData.getTables(null, null, null, null);
        databaseMetaData.getColumns(null, null, null, null);

        for (final String query : queries) {
            runQueryPrintResults(query, connection.createStatement());
        }
    }

    @Test
    @Disabled
    void testSchema() throws SQLException {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set default to IAMSigV4
        properties.put(CONTACT_POINT_KEY, ENDPOINT);
        properties.put(PORT_KEY, PORT);
        properties.put(ENABLE_SSL_KEY, true);

        final java.sql.Connection connection = new SqlGremlinConnection(new GremlinConnectionProperties(properties));
        final java.sql.Statement statement = connection.createStatement();

        runQueryPrintResults("SELECT category FROM websiteGroup LIMIT 5", statement);
        runQueryPrintResults("SELECT title FROM website LIMIT 10", statement);
        runQueryPrintResults("SELECT visited_id FROM website LIMIT 5", statement);
        runQueryPrintResults("SELECT visited_id, title FROM website LIMIT 15", statement);
        runQueryPrintResults("SELECT title, visited_id FROM website LIMIT 15", statement);
        runQueryPrintResults("SELECT * FROM website LIMIT 10", statement);
        runQueryPrintResults(
                "SELECT `website`.`url` AS `url` FROM `website` INNER JOIN `transientId` ON " +
                        "(`website`.`visited_id` = `transientId`.`visited_id`) LIMIT 5",
                statement);
        runQueryPrintResults(
                "SELECT `transientId`.`os` AS `os` FROM `transientId` INNER JOIN `website` ON " +
                        "(`website`.`visited_id` = `transientId`.`visited_id`) LIMIT 10",
                statement);
    }

    void runQueryPrintResults(final String query, final java.sql.Statement statement) throws SQLException {
        System.out.println("Executing query: " + query);
        final java.sql.ResultSet resultSet = statement.executeQuery(query);
        final int columnCount = resultSet.getMetaData().getColumnCount();
        final List<String> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            columns.add(resultSet.getMetaData().getColumnName(i));
        }

        final List<List<Object>> rows = new ArrayList<>();
        while (resultSet.next()) {
            final List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(resultSet.getObject(i));
            }
            rows.add(row);
        }

        final Object[][] rowObjects = new Object[rows.size()][];
        final String[] colString = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            colString[i] = columns.get(i);
        }
        for (int i = 0; i < rows.size(); i++) {
            rowObjects[i] = rows.get(i) == null ? null : rows.get(i).toArray();
        }

        final TextTable tt = new TextTable(colString, rowObjects);
        tt.printTable();
    }
}
