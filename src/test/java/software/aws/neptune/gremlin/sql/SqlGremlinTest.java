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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import dnl.utils.text.table.TextTable;
import lombok.AllArgsConstructor;
import org.apache.calcite.util.Pair;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.SigV4WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.twilmes.sql.gremlin.adapter.converter.SqlConverter;
import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinEdgeTable;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinProperty;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinVertexTable;
import org.twilmes.sql.gremlin.adapter.results.SqlGremlinQueryResult;
import software.aws.neptune.gremlin.GremlinConnection;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.jdbc.utilities.ConnectionProperties;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.CONTACT_POINT_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.ENABLE_SSL_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.PORT_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.SERVICE_REGION_KEY;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SCAN_TYPE_KEY;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_HOSTNAME;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_PRIVATE_KEY_FILE;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_STRICT_HOST_KEY_CHECKING;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.SSH_USER;

// Temporary test file to do ad hoc testing.
@Disabled
public class SqlGremlinTest {
    private static final String ENDPOINT = "jdbc-bug-bash-iam-instance-1.cdubgfjknn5r.us-east-1.neptune.amazonaws.com";
    private static final int PORT = 8182;
    private static final Map<Class<?>, String> TYPE_MAP = new HashMap<>();

    static {
        TYPE_MAP.put(String.class, "String");
        TYPE_MAP.put(Boolean.class, "Boolean");
        TYPE_MAP.put(Byte.class, "Byte");
        TYPE_MAP.put(Short.class, "Short");
        TYPE_MAP.put(Integer.class, "Integer");
        TYPE_MAP.put(Long.class, "Long");
        TYPE_MAP.put(Float.class, "Float");
        TYPE_MAP.put(Double.class, "Double");
        TYPE_MAP.put(Date.class, "Date");
    }

    private static String getType(final Set<Object> data) {
        final Set<String> types = new HashSet<>();
        for (final Object d : data) {
            types.add(TYPE_MAP.getOrDefault(d.getClass(), "String"));
        }
        if (types.size() == 1) {
            return types.iterator().next();
        }
        return "String";
    }

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
        properties.put(SERVICE_REGION_KEY, "us-east-1");
        properties.put(SSH_USER, "ec2-user");
        properties.put(SSH_HOSTNAME, "52.14.185.245");
        properties.put(SSH_PRIVATE_KEY_FILE, "~/Downloads/EC2/neptune-test.pem");
        properties.put(SSH_STRICT_HOST_KEY_CHECKING, "false");
        properties.put(SCAN_TYPE_KEY, "first");

        final List<String> queries = ImmutableList.of(
                "SELECT count(region) from airport limit 10"
        /* "SELECT COUNT(\"Calcs\".\"bool0\") AS \"TEMP(Test)(1133866179)(0)\"\n" +
                        "FROM \"gremlin\".\"Calcs\" \"Calcs\"\n" +
                        "HAVING (COUNT(1) > 0)",
                "SELECT COUNT(`Calcs`.`int0`) AS `TEMP(Test)(3910975586)(0)` " +
                        "FROM `gremlin`.`Calcs` `Calcs` " +
                        "HAVING (COUNT(1) > 0)",
                "SELECT \"Staples\".\"Discount\" AS \"TEMP(Test)(1913174168)(0)\" " +
                        "FROM \"gremlin\".\"Staples\" \"Staples\" " +
                        "GROUP BY \"Staples\".\"Discount\"",
                "SELECT `airport`.`country`, `airport`.`region` FROM `airport` ORDER BY `airport`.`country`, `airport`.`region` DESC LIMIT 10",
                "SELECT `airport`.`country`, `airport`.`region` FROM `airport` ORDER BY `airport`.`country`, `airport`.`region` ASC LIMIT 10",
                "SELECT \"Calcs\".\"Calcs_ID\" AS \"Calcs_ID\", \"Calcs\".\"bool0\" AS \"bool_\", \"Calcs\".\"bool1\" AS \"bool_1\", \"Calcs\".\"bool2\" AS \"bool_2\", \"Calcs\".\"bool3\" AS \"bool_3\", \"Calcs\".\"date0\" AS \"date_\", \"Calcs\".\"date1\" AS \"date_1\", \"Calcs\".\"date2\" AS \"date_2\", \"Calcs\".\"date3\" AS \"date_3\", \"Calcs\".\"datetime0\" AS \"datetime_\", \"Calcs\".\"datetime1\" AS \"datetime_1\", \"Calcs\".\"int0\" AS \"int_\", \"Calcs\".\"int1\" AS \"int_1\", \"Calcs\".\"int2\" AS \"int_2\", \"Calcs\".\"int3\" AS \"int_3\", \"Calcs\".\"key\" AS \"key\", \"Calcs\".\"num0\" AS \"num_\", \"Calcs\".\"num1\" AS \"num_1\", \"Calcs\".\"num2\" AS \"num_2\", \"Calcs\".\"num3\" AS \"num_3\", \"Calcs\".\"num4\" AS \"num_4\", \"Calcs\".\"str0\" AS \"str_\", \"Calcs\".\"str1\" AS \"str_1\", \"Calcs\".\"str2\" AS \"str_2\", \"Calcs\".\"str3\" AS \"str_3\", \"Calcs\".\"time0\" AS \"time_\", \"Calcs\".\"time1\" AS \"time_1\", \"Calcs\".\"zzz\" AS \"zzz\" FROM \"gremlin\".\"Calcs\" \"Calcs\" LIMIT 10000 ",
                "SELECT \n" +
                        "     `airport`.`city` AS `city`,\n" +
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
                        " GROUP BY `airport`.`lat` LIMIT 100",
                "SELECT `airport`.`country`, `airport`.`region` FROM `airport` ORDER BY `airport`.`country`, `airport`.`region` LIMIT 100",
                "SELECT `airport`.`code` AS `code` FROM `gremlin`.`airport` `airport` " +
                        "INNER JOIN `gremlin`.`airport` `airport1` " +
                        "ON (`airport`.`route_IN_ID` = `airport1`.`route_OUT_ID`) " +
                        "GROUP BY `airport`.`code`",
                "SELECT " +
                        "                     `airport1`.`ROUTE_OUT_ID` as `arid1`, " +
                        "                     `airport`.`ROUTE_IN_ID` as `arid0`, " +
                        "                     `airport1`.`city` AS `city__airport__`, " +
                        "                     `airport`.`city` AS `city`, " +
                        "                     AVG(`airport`.`lat`) AS `avg`" +
                        "                           FROM `gremlin`.`airport` `airport` " +
                        "                               INNER JOIN `gremlin`.`airport` `airport1` " +
                        "                                   ON (`airport`.`ROUTE_IN_ID` = `airport1`.`ROUTE_OUT_ID`) " +
                        "                                       GROUP BY `airport`.`lat`, `airport1`.`city`, `airport`.`city`," +
                        "                                                 `airport1`.`ROUTE_OUT_ID`, `airport`.`ROUTE_IN_ID`" +
                        "   LIMIT 100",
                "SELECT \"route\".\"dist\" AS \"dist\" FROM \"gremlin\".\"route\" \"route\" LIMIT 100",
                "SELECT `airport`.`country`, `airport`.`region`, " +
                        " SUM(`airport`.`elev`) AS `elev_sum`, " +
                        "AVG(`airport`.`elev`) AS `elev_avg`, " +
                        "MIN(`airport`.`elev`) AS `elev_min`, " +
                        "MAX(`airport`.`elev`) AS `elev_max`," +
                        " SUM(`airport`.`lat`) AS `lat_sum`, " +
                        "AVG(`airport`.`lat`) AS `lat_avg`, " +
                        "MIN(`airport`.`lat`) AS `lat_min`, " +
                        "MAX(`airport`.`lat`) AS `lat_max` " +
                        " FROM `airport` GROUP BY `airport`.`country`, `airport`.`region`",
                "SELECT " +
                        "                     `airport1`.`ROUTE_OUT_ID` as `arid1`, " +
                        "                     `airport`.`ROUTE_IN_ID` as `arid0`, " +
                        "                     `airport1`.`city` AS `city__airport__`, " +
                        "                     `airport`.`city` AS `city`, " +
                        "                     AVG(`airport`.`lat`) AS `avg`" +
                        "                           FROM `gremlin`.`airport` `airport` " +
                        "                               INNER JOIN `gremlin`.`airport` `airport1` " +
                        "                                   ON (`airport`.`ROUTE_IN_ID` = `airport1`.`ROUTE_OUT_ID`) " +
                        "                                       GROUP BY `airport`.`lat`, `airport1`.`city`, `airport`.`city`," +
                        "                                                 `airport1`.`ROUTE_OUT_ID`, `airport`.`ROUTE_IN_ID`" +
                        "               ORDER BY `arid1`, `arid0`, `city__airport__`, `city`" +
                        "   LIMIT 100" */
        );

        // Issue is in ROUTE / ROUTE_OUT_ID
        final java.sql.Connection connection = new SqlGremlinConnection(new GremlinConnectionProperties(properties));
        for (final String query : queries) {
            runQueryPrintResults(query, connection.createStatement());
        }
    }

    @Test
    void testHaving() throws SQLException {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set default to IAMSigV4
        properties.put(CONTACT_POINT_KEY, ENDPOINT);
        properties.put(PORT_KEY, PORT);
        properties.put(ENABLE_SSL_KEY, true);
        properties.put(SSH_USER, "ec2-user");
        properties.put(SSH_HOSTNAME, "52.14.185.245");
        properties.put(SSH_PRIVATE_KEY_FILE, "~/Downloads/EC2/neptune-test.pem");
        properties.put(SSH_STRICT_HOST_KEY_CHECKING, "false");
        properties.put(SCAN_TYPE_KEY, "first");

        final List<String> queries = ImmutableList.of(
                /*"SELECT `airport`.`country`, `airport`.`region`, " +
                        " SUM(`airport`.`elev`) AS `elev_sum`, " +
                        "AVG(`airport`.`elev`) AS `elev_avg`, " +
                        "MIN(`airport`.`elev`) AS `elev_min`, " +
                        "MAX(`airport`.`elev`) AS `elev_max`," +
                        " SUM(`airport`.`lat`) AS `lat_sum`, " +
                        "AVG(`airport`.`lat`) AS `lat_avg`, " +
                        "MIN(`airport`.`lat`) AS `lat_min`, " +
                        "MAX(`airport`.`lat`) AS `lat_max` " +
                        " FROM `airport` GROUP BY `airport`.`country`, `airport`.`region` ORDER BY SUM(`airport`.`elev`)",*/
                "SELECT `airport`.`country` FROM `airport` GROUP BY `airport`.`country` " +
                        " HAVING (COUNT(1) > 0) LIMIT 10",
                "SELECT `airport`.`country`, `airport`.`region`, " +
                        " SUM(`airport`.`elev`) AS `elev_sum`, " +
                        "AVG(`airport`.`elev`) AS `elev_avg`, " +
                        "MIN(`airport`.`elev`) AS `elev_min`, " +
                        "MAX(`airport`.`elev`) AS `elev_max`," +
                        " SUM(`airport`.`lat`) AS `lat_sum`, " +
                        "AVG(`airport`.`lat`) AS `lat_avg`, " +
                        "MIN(`airport`.`lat`) AS `lat_min`, " +
                        "MAX(`airport`.`lat`) AS `lat_max` " +
                        " FROM `airport` GROUP BY `airport`.`country`, `airport`.`region`, `airport`.`elev`, `airport`.`lat`" +
                        " HAVING (COUNT(1) > 0) LIMIT 10",
                "SELECT `airport`.`country`, `airport`.`region`, " +
                        " SUM(`airport`.`elev`) AS `elev_sum`, " +
                        "AVG(`airport`.`elev`) AS `elev_avg`, " +
                        "MIN(`airport`.`elev`) AS `elev_min`, " +
                        "MAX(`airport`.`elev`) AS `elev_max`," +
                        " SUM(`airport`.`lat`) AS `lat_sum`, " +
                        "AVG(`airport`.`lat`) AS `lat_avg`, " +
                        "MIN(`airport`.`lat`) AS `lat_min`, " +
                        "MAX(`airport`.`lat`) AS `lat_max` " +
                        " FROM `airport` GROUP BY `airport`.`country`, `airport`.`region`, `airport`.`elev`, `airport`.`lat`" +
                        " HAVING `airport`.`lat` >= 8 LIMIT 10",
                "SELECT `airport`.`country`, `airport`.`region`, " +
                        " SUM(`airport`.`elev`) AS `elev_sum`, " +
                        "AVG(`airport`.`elev`) AS `elev_avg`, " +
                        "MIN(`airport`.`elev`) AS `elev_min`, " +
                        "MAX(`airport`.`elev`) AS `elev_max`," +
                        " SUM(`airport`.`lat`) AS `lat_sum`, " +
                        "AVG(`airport`.`lat`) AS `lat_avg`, " +
                        "MIN(`airport`.`lat`) AS `lat_min`, " +
                        "MAX(`airport`.`lat`) AS `lat_max` " +
                        " FROM `airport` GROUP BY `airport`.`country`, `airport`.`region`, `airport`.`elev`, `airport`.`lat`" +
                        " HAVING `airport`.`lat` <= 8 LIMIT 10",
                "SELECT `airport`.`lat`, `airport`.`lon`, " +
                        " SUM(`airport`.`elev`) AS `elev_sum`, " +
                        "AVG(`airport`.`elev`) AS `elev_avg`, " +
                        "MIN(`airport`.`elev`) AS `elev_min`, " +
                        "MAX(`airport`.`elev`) AS `elev_max`," +
                        " SUM(`airport`.`lat`) AS `lat_sum`, " +
                        "AVG(`airport`.`lat`) AS `lat_avg`, " +
                        "MIN(`airport`.`lat`) AS `lat_min`, " +
                        "MAX(`airport`.`lat`) AS `lat_max` " +
                        " FROM `airport` GROUP BY `airport`.`country`, `airport`.`region`, `airport`.`elev`, `airport`.`lat`, `airport`.`lon`" +
                        " HAVING COUNT(`airport`.`city`) > 8 LIMIT 10",
                "SELECT `airport`.`lat`, `airport`.`lon`, " +
                        " SUM(`airport`.`elev`) AS `elev_sum`, " +
                        "AVG(`airport`.`elev`) AS `elev_avg`, " +
                        "MIN(`airport`.`elev`) AS `elev_min`, " +
                        "MAX(`airport`.`elev`) AS `elev_max`," +
                        " SUM(`airport`.`lat`) AS `lat_sum`, " +
                        "AVG(`airport`.`lat`) AS `lat_avg`, " +
                        "MIN(`airport`.`lat`) AS `lat_min`, " +
                        "MAX(`airport`.`lat`) AS `lat_max` " +
                        " FROM `airport` GROUP BY `airport`.`lat`, `airport`.`lon`, `airport`.`country`, `airport`.`region`" +
                        " HAVING `airport`.`lat` > `airport`.`lon` LIMIT 10",
                "SELECT `airport`.`lat`, `airport`.`lon`, " +
                        " SUM(`airport`.`elev`) AS `elev_sum`, " +
                        "AVG(`airport`.`elev`) AS `elev_avg`, " +
                        "MIN(`airport`.`elev`) AS `elev_min`, " +
                        "MAX(`airport`.`elev`) AS `elev_max`," +
                        " SUM(`airport`.`lat`) AS `lat_sum`, " +
                        "AVG(`airport`.`lat`) AS `lat_avg`, " +
                        "MIN(`airport`.`lat`) AS `lat_min`, " +
                        "MAX(`airport`.`lat`) AS `lat_max` " +
                        " FROM `airport` GROUP BY `airport`.`country`, `airport`.`region`, `airport`.`elev`, `airport`.`lat`, `airport`.`lon`" +
                        " HAVING `airport`.`lat` > 8 LIMIT 10",
                "SELECT `airport`.`country`, `airport`.`region`, " +
                        " SUM(`airport`.`elev`) AS `elev_sum`, " +
                        "AVG(`airport`.`elev`) AS `elev_avg`, " +
                        "MIN(`airport`.`elev`) AS `elev_min`, " +
                        "MAX(`airport`.`elev`) AS `elev_max`," +
                        " SUM(`airport`.`lat`) AS `lat_sum`, " +
                        "AVG(`airport`.`lat`) AS `lat_avg`, " +
                        "MIN(`airport`.`lat`) AS `lat_min`, " +
                        "MAX(`airport`.`lat`) AS `lat_max` " +
                        " FROM `airport` GROUP BY `airport`.`country`, `airport`.`region`, `airport`.`elev`, `airport`.`lat`" +
                        " HAVING `airport`.`lat` > 8 LIMIT 10"
        );

        // Issue is in ROUTE / ROUTE_OUT_ID
        final java.sql.Connection connection = new SqlGremlinConnection(new GremlinConnectionProperties(properties));
        for (final String query : queries) {
            runQueryPrintResults(query, connection.createStatement());
        }
    }

    @Test
    void testAggregateFunctions() throws SQLException {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set default to IAMSigV4
        properties.put(CONTACT_POINT_KEY, ENDPOINT);
        properties.put(PORT_KEY, PORT);
        properties.put(ENABLE_SSL_KEY, true);
        properties.put(SSH_USER, "ec2-user");
        properties.put(SSH_HOSTNAME, "52.14.185.245");
        properties.put(SSH_PRIVATE_KEY_FILE, "~/Downloads/EC2/neptune-test.pem");
        properties.put(SSH_STRICT_HOST_KEY_CHECKING, "false");
        properties.put(SCAN_TYPE_KEY, "first");

        final Properties gremlinProperties = new Properties();
        gremlinProperties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set default to IAMSigV4
        gremlinProperties.put(CONTACT_POINT_KEY, ENDPOINT);
        gremlinProperties.put(PORT_KEY, PORT);

        final List<Pair<String, String>> queries = ImmutableList.of(
                new Pair<>(generateAggregateSQLQueries("AVG", "airport", "lat"),
                        generateAggregateGremlinQueries("mean", "airport", "lat")),
                new Pair<>(generateAggregateSQLQueries("SUM", "airport", "lat"),
                        generateAggregateGremlinQueries("sum", "airport", "lat")),
                new Pair<>(generateAggregateSQLQueries("COUNT", "airport", "lat"),
                        generateAggregateGremlinQueries("count", "airport", "lat")),
                new Pair<>(generateAggregateSQLQueries("MAX", "airport", "lat"),
                        generateAggregateGremlinQueries("max", "airport", "lat")),
                new Pair<>(generateAggregateSQLQueries("MIN", "airport", "lat"),
                        generateAggregateGremlinQueries("min", "airport", "lat")),

                new Pair<>(generateAggregateSQLQueries("AVG", "airport", "elev"),
                        generateAggregateGremlinQueries("mean", "airport", "elev")),
                new Pair<>(generateAggregateSQLQueries("SUM", "airport", "elev"),
                        generateAggregateGremlinQueries("sum", "airport", "elev")),
                new Pair<>(generateAggregateSQLQueries("COUNT", "airport", "elev"),
                        generateAggregateGremlinQueries("count", "airport", "elev")),
                new Pair<>(generateAggregateSQLQueries("MAX", "airport", "elev"),
                        generateAggregateGremlinQueries("max", "airport", "elev")),
                new Pair<>(generateAggregateSQLQueries("MIN", "airport", "elev"),
                        generateAggregateGremlinQueries("min", "airport", "elev"))
        );

        final List<Pair<String, String>> groupQuery = ImmutableList.of(
                new Pair<>(generateAggregateSQLQueriesWithGroup("AVG", "airport", "lat", "country"),
                        generateAggregateGremlinQueriesWithGroup("mean", "airport", "lat", "country")),
                new Pair<>(generateAggregateSQLQueriesWithGroup("SUM", "airport", "lat", "country"),
                        generateAggregateGremlinQueriesWithGroup("sum", "airport", "lat", "country")),
                new Pair<>(generateAggregateSQLQueriesWithGroup("COUNT", "airport", "lat", "country"),
                        generateAggregateGremlinQueriesWithGroup("count", "airport", "lat", "country")),
                new Pair<>(generateAggregateSQLQueriesWithGroup("MAX", "airport", "lat", "country"),
                        generateAggregateGremlinQueriesWithGroup("max", "airport", "lat", "country")),
                new Pair<>(generateAggregateSQLQueriesWithGroup("MIN", "airport", "lat", "country"),
                        generateAggregateGremlinQueriesWithGroup("min", "airport", "lat", "country")),

                new Pair<>(generateAggregateSQLQueriesWithGroup("AVG", "airport", "elev", "country"),
                        generateAggregateGremlinQueriesWithGroup("mean", "airport", "elev", "country")),
                new Pair<>(generateAggregateSQLQueriesWithGroup("SUM", "airport", "elev", "country"),
                        generateAggregateGremlinQueriesWithGroup("sum", "airport", "elev", "country")),
                new Pair<>(generateAggregateSQLQueriesWithGroup("COUNT", "airport", "elev", "country"),
                        generateAggregateGremlinQueriesWithGroup("count", "airport", "elev", "country")),
                new Pair<>(generateAggregateSQLQueriesWithGroup("MAX", "airport", "elev", "country"),
                        generateAggregateGremlinQueriesWithGroup("max", "airport", "elev", "country")),
                new Pair<>(generateAggregateSQLQueriesWithGroup("MIN", "airport", "elev", "country"),
                        generateAggregateGremlinQueriesWithGroup("min", "airport", "elev", "country"))
        );
        final GremlinConnectionProperties sqlGremlinConnectionProperties = new GremlinConnectionProperties(properties);
        final java.sql.Connection sqlGremlinConnection = new SqlGremlinConnection(sqlGremlinConnectionProperties);

        final GremlinConnectionProperties gremlinConnectionProperties =
                new GremlinConnectionProperties(gremlinProperties);
        gremlinConnectionProperties.sshTunnelOverride(sqlGremlinConnectionProperties.getPort());
        final java.sql.Connection gremlinConnection = new GremlinConnection(gremlinConnectionProperties);

        for (final Pair<String, String> query : queries) {
            runQueriesCompareResults(query.getKey(), query.getValue(), sqlGremlinConnection.createStatement(),
                    gremlinConnection.createStatement());
        }
    }

    private String generateAggregateSQLQueries(final String operator, final String table, final String column) {
        // Returns a simple SQL query in the form:
        // "SELECT OPERATOR(`table`.`column`) AS `OPERATOR` FROM `gremlin`.`table` `table` GROUP BY `table`.`column`"
        return String.format("SELECT %s(`%s`.`%s`) AS `%s` FROM `gremlin`.`%s` `%s` GROUP BY `%s`.`%s`",
                operator, table, column, operator, table, table, table, column);
    }


    private String generateAggregateGremlinQueriesWithGroup(final String operator, final String table,
                                                            final String column, final String groupColumn) {
        // Returns a simple Gremlin query in the form:
        return String.format(
                "g.V().hasLabel('%s')." +
                        "group()." +
                        "by(union(values('%s')).fold()).unfold()." +
                        "select(values)." +
                        "order()." +
                        "by(unfold().values())." +
                        "project('%s_%s')." +
                        "by(unfold().values('%s').%s())",
                table, groupColumn, column, operator.toLowerCase(), column, operator.toLowerCase());
    }

    private String generateAggregateSQLQueriesWithGroup(final String operator, final String table, final String column,
                                                        final String groupColumn) {
        // Returns a simple SQL query in the form:
        // "SELECT OPERATOR(`table`.`column`) AS `OPERATOR` FROM `gremlin`.`table` `table` GROUP BY `table`.`column`"
        return String.format("SELECT `%s`.`%s` %s(`%s`.`%s`) AS `%s` FROM `gremlin`.`%s` `%s` GROUP BY `%s`.`%s`",
                table, groupColumn, operator, table, column, operator, table, table, table, groupColumn);
    }


    private String generateAggregateGremlinQueries(final String operator, final String table, final String column) {
        // Returns a simple Gremlin query in the form:
        return String
                .format("g.V().hasLabel('%s').group().by(union(values('%s')).fold()).unfold().select(values).order().by(unfold().values())" +
                                ".fold().project('%s_%s').by(unfold().unfold().values('%s').%s())",
                        table, column, column, operator.toLowerCase(), column, operator.toLowerCase());
    }

    void runQueriesCompareResults(final String sqlQuery, final String gremlinQuery,
                                  final java.sql.Statement sqlGremlinStatement,
                                  final java.sql.Statement gremlinStatement) throws SQLException {
        System.out.println("SQL: " + sqlQuery);
        System.out.println("Gremlin: " + gremlinQuery);
        final java.sql.ResultSet sqlGremlinResultSet = sqlGremlinStatement.executeQuery(sqlQuery);
        final java.sql.ResultSet gremlinResultSet = gremlinStatement.executeQuery(gremlinQuery);

        final int sqlGremlinColumnCount = sqlGremlinResultSet.getMetaData().getColumnCount();
        final int gremlinColumnCount = gremlinResultSet.getMetaData().getColumnCount();

        // we expect aggregate results to be of 1 column
        Assertions.assertEquals(sqlGremlinColumnCount, 1);
        Assertions.assertEquals(gremlinColumnCount, 1);

        while (sqlGremlinResultSet.next() && gremlinResultSet.next()) {
            Assertions.assertEquals(sqlGremlinResultSet.getObject(1), gremlinResultSet.getObject(1));
        }
    }

    @Test
    void testSqlConnectionExecution() throws SQLException {
        final Properties properties = new Properties();
        properties.put(ConnectionProperties.AUTH_SCHEME_KEY, AuthScheme.IAMSigV4); // set default to IAMSigV4
        properties.put(CONTACT_POINT_KEY, ENDPOINT);
        properties.put(PORT_KEY, PORT);
        properties.put(ENABLE_SSL_KEY, true);
        properties.put(SSH_USER, "ec2-user");
        properties.put(SSH_HOSTNAME, "52.14.185.245");
        properties.put(SSH_PRIVATE_KEY_FILE, "~/Downloads/EC2/neptune-test.pem");
        properties.put(SSH_STRICT_HOST_KEY_CHECKING, "false");
        properties.put(SCAN_TYPE_KEY, "first");

        final List<String> queries = ImmutableList.of("SELECT * FROM Person",
                "SELECT `Person`.`firstName` AS `firstName`, `Cat`.`name` AS `name` FROM `Cat` INNER JOIN `Person` ON (`Cat`.`name` = `Person`.`name`) GROUP BY `Person`.`firstName`, `Cat`.`name`",
                "SELECT `Person`.`age` AS `age`, `Cat`.`name` AS `name__Cat_` FROM `Person` INNER JOIN `Cat` ON (`Person`.`firstName` = `Cat`.`name`) GROUP BY `Person`.`age`, `Cat`.`name`",
                "SELECT `Person`.`firstName` AS `firstName`, `Cat`.`name` AS `name` FROM `Cat` INNER JOIN `Person` ON `Cat`.`name` = `Person`.`name`",
                "SELECT `Person`.`age` AS `age`, SUM(1) AS `cnt_Person_4A9569D21233471BB4DC6258F15087AD_ok`, `Person`.`pets` AS `pets` FROM `Person` GROUP BY `Person`.`age`, `Person`.`pets`",
                "SELECT `Person`.`age` AS `age`, `Person`.`cat` AS `cat`, `Person`.`dog` AS `dog`, `Person`.`firstName` AS `firstName`, `Person`.`lastName` AS `lastName`, `Person`.`name` AS `name`, `Person`.`pets` AS `pets`, `Person`.`title` AS `title` FROM `Person` LIMIT 10000");

        final java.sql.Connection connection = new SqlGremlinConnection(new GremlinConnectionProperties(properties));
        final java.sql.DatabaseMetaData databaseMetaData = connection.getMetaData();
        databaseMetaData.getTables(null, null, null, null);
        final java.sql.ResultSet columns = databaseMetaData.getColumns(null, null, null, null);
        while (columns.next()) {
            System.out.println("Column " + columns.getString("COLUMN_NAME") + " - " + columns.getString("TYPE_NAME")
                    + " - " + columns.getString("DATA_TYPE"));
        }
        for (final String query : queries) {
            runQueryPrintResults(query, connection.createStatement());
        }
    }

    @Test
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

    @Test
    void getGremlinSchema() throws Exception {
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

        final java.sql.Connection connection = new SqlGremlinConnection(gremlinConnectionProperties);

        final Cluster cluster = SqlGremlinQueryExecutor.createClusterBuilder(gremlinConnectionProperties).create();
        final Client client = cluster.connect().init();

        final ExecutorService executor = Executors.newFixedThreadPool(96,
                new ThreadFactoryBuilder().setNameFormat("RxSessionRunner-%d").setDaemon(true).build());

        final Future<List<GremlinVertexTable>> gremlinVertexTablesFuture =
                executor.submit(new RunGremlinQueryVertices(client, executor, TraversalStrategy.First));
        final Future<List<GremlinEdgeTable>> gremlinEdgeTablesFuture =
                executor.submit(new RunGremlinQueryEdges(client, executor, TraversalStrategy.First));
        final List<GremlinVertexTable> vertices = gremlinVertexTablesFuture.get();
        final List<GremlinEdgeTable> edges = gremlinEdgeTablesFuture.get();
        final GremlinSchema gremlinSchema = new GremlinSchema(vertices, edges);
        final SqlConverter sqlConverter =
                new SqlConverter(gremlinSchema, traversal().withRemote(DriverRemoteConnection.using(client)));
        SqlGremlinQueryResult result = sqlConverter.executeQuery("SELECT airport.city AS c FROM airport");
        System.out.println("Columns: " + result.getColumns());
        result = sqlConverter.executeQuery("SELECT city AS c FROM airport");
        System.out.println("Columns: " + result.getColumns());
        result = sqlConverter.executeQuery("SELECT city FROM airport HAVING (COUNT(1) > 0)");
        System.out.println("Columns: " + result.getColumns());
    }

    enum TraversalStrategy {
        First,
        RandomN,
        All
    }

    @AllArgsConstructor
    class RunGremlinQueryVertices implements Callable<List<GremlinVertexTable>> {
        private final Client client;
        private final ExecutorService service;
        private final TraversalStrategy traversalStrategy;

        @Override
        public List<GremlinVertexTable> call() throws Exception {
            final List<Future<List<GremlinProperty>>> gremlinProperties = new ArrayList<>();
            final List<Future<List<String>>> gremlinVertexInEdgeLabels = new ArrayList<>();
            final List<Future<List<String>>> gremlinVertexOutEdgeLabels = new ArrayList<>();
            final List<String> labels = service.submit(new RunGremlinQueryLabels(true, client)).get();

            for (final String label : labels) {
                gremlinProperties.add(service.submit(
                        new RunGremlinQueryPropertiesList(true, label, client, traversalStrategy, service)));
                gremlinVertexInEdgeLabels.add(service.submit(new RunGremlinQueryVertexEdges(client, label, "in")));
                gremlinVertexOutEdgeLabels.add(service.submit(new RunGremlinQueryVertexEdges(client, label, "out")));
            }

            final List<GremlinVertexTable> gremlinVertexTables = new ArrayList<>();
            for (int i = 0; i < labels.size(); i++) {
                gremlinVertexTables.add(new GremlinVertexTable(labels.get(i), gremlinProperties.get(i).get(),
                        gremlinVertexInEdgeLabels.get(i).get(), gremlinVertexOutEdgeLabels.get(i).get()));
            }
            return gremlinVertexTables;
        }
    }

    @AllArgsConstructor
    class RunGremlinQueryVertexEdges implements Callable<List<String>> {
        private final Client client;
        private final String label;
        private final String direction;

        @Override
        public List<String> call() throws Exception {
            final String query = String.format("g.V().hasLabel('%s').%sE().label().dedup()", label, direction);
            System.out.printf("Start %s%n", query);
            final ResultSet resultSet = client.submit(query);
            final List<String> labels = new ArrayList<>();
            resultSet.stream().iterator().forEachRemaining(it -> labels.add(it.getString()));
            System.out.printf("End %s%n", query);
            return labels;
        }
    }


    @AllArgsConstructor
    class RunGremlinQueryEdges implements Callable<List<GremlinEdgeTable>> {
        private final Client client;
        private final ExecutorService service;
        private final TraversalStrategy traversalStrategy;

        @Override
        public List<GremlinEdgeTable> call() throws Exception {
            final List<Future<List<GremlinProperty>>> futureTableColumns = new ArrayList<>();
            final List<Future<List<Pair<String, String>>>> inOutLabels = new ArrayList<>();
            final List<String> labels = service.submit(new RunGremlinQueryLabels(false, client)).get();

            for (final String label : labels) {
                futureTableColumns.add(service.submit(
                        new RunGremlinQueryPropertiesList(false, label, client, traversalStrategy, service)));
                inOutLabels.add(service.submit(new RunGremlinQueryInOutV(client, label)));
            }

            final List<GremlinEdgeTable> gremlinEdgeTables = new ArrayList<>();
            for (int i = 0; i < labels.size(); i++) {
                gremlinEdgeTables.add(new GremlinEdgeTable(labels.get(i), futureTableColumns.get(i).get(),
                        inOutLabels.get(i).get()));
            }
            return gremlinEdgeTables;
        }
    }

    @AllArgsConstructor
    class RunGremlinQueryPropertyType implements Callable<String> {
        private final boolean isVertex;
        private final String label;
        private final String property;
        private final Client client;
        private final TraversalStrategy strategy;

        @Override
        public String call() {
            final String query;
            if (strategy.equals(TraversalStrategy.First)) {
                query = String.format("g.%s().hasLabel('%s').values('%s').next(1)", isVertex ? "V" : "E", label,
                        property);
            } else {
                query = String.format("g.%s().hasLabel('%s').values('%s').toSet()", isVertex ? "V" : "E", label,
                        property);
            }
            System.out.printf("Start %s%n", query);

            final ResultSet resultSet = client.submit(query);
            final Set<Object> data = new HashSet<>();
            resultSet.stream().iterator().forEachRemaining(data::add);
            System.out.printf("End %s%n", query);
            return getType(data);
        }
    }

    @AllArgsConstructor
    class RunGremlinQueryPropertiesList implements Callable<List<GremlinProperty>> {
        private final boolean isVertex;
        private final String label;
        private final Client client;
        private final TraversalStrategy traversalStrategy;
        private final ExecutorService service;

        @Override
        public List<GremlinProperty> call() throws ExecutionException, InterruptedException {
            final String query =
                    String.format("g.%s().hasLabel('%s').properties().key().dedup()", isVertex ? "V" : "E", label);
            System.out.printf("Start %s%n", query);
            final ResultSet resultSet = client.submit(query);
            final Iterator<Result> iterator = resultSet.stream().iterator();
            final List<String> properties = new ArrayList<>();
            final List<Future<String>> propertyTypes = new ArrayList<>();
            while (iterator.hasNext()) {
                final String property = iterator.next().getString();
                propertyTypes.add(service
                        .submit(new RunGremlinQueryPropertyType(isVertex, label, property, client, traversalStrategy)));
                properties.add(property);
            }

            final List<GremlinProperty> columns = new ArrayList<>();
            for (int i = 0; i < properties.size(); i++) {
                columns.add(new GremlinProperty(properties.get(i), propertyTypes.get(i).get().toLowerCase()));
            }

            System.out.printf("End %s%n", query);
            return columns;
        }
    }

    @AllArgsConstructor
    class RunGremlinQueryLabels implements Callable<List<String>> {
        private final boolean isVertex;
        private final Client client;

        @Override
        public List<String> call() throws Exception {
            // Get labels.
            final String query = String.format("g.%s().label().dedup()", isVertex ? "V" : "E");
            System.out.printf("Start %s%n", query);
            final List<String> labels = new ArrayList<>();
            final ResultSet resultSet = client.submit(query);
            resultSet.stream().iterator().forEachRemaining(it -> labels.add(it.getString()));
            System.out.printf("End %s%n", query);
            return labels;
        }
    }

    @AllArgsConstructor
    class RunGremlinQueryInOutV implements Callable<List<Pair<String, String>>> {
        private final Client client;
        private final String label;

        @Override
        public List<Pair<String, String>> call() throws Exception {
            // Get labels.
            final String query = String.format(
                    "g.E().hasLabel('%s').project('in','out').by(inV().label()).by(outV().label()).dedup()", label);
            System.out.printf("Start %s%n", query);
            final List<Pair<String, String>> labels = new ArrayList<>();
            final ResultSet resultSet = client.submit(query);
            resultSet.stream().iterator().forEachRemaining(map -> {
                final Map<String, String> m = (Map<String, String>) map.getObject();
                m.forEach((key, value) -> labels.add(new Pair<>(key, value)));
            });
            System.out.printf("End %s%n", query);
            return labels;
        }
    }

    @AllArgsConstructor
    class RunGremlinQueryOutV implements Callable<List<String>> {
        private final Client client;
        private final String label;

        @Override
        public List<String> call() throws Exception {
            // Get labels.
            final String query = String.format("g.E().hasLabel('%s').out().label().dedup()", label);
            System.out.printf("Start %s%n", query);
            final List<String> labels = new ArrayList<>();
            final ResultSet resultSet = client.submit(query);
            resultSet.stream().iterator().forEachRemaining(it -> labels.add(it.getString()));
            System.out.printf("End %s%n", query);
            return labels;
        }
    }
}
