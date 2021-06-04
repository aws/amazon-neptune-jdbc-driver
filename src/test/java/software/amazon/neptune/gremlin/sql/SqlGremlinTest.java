/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.neptune.gremlin.sql;

import com.github.javafaker.Address;
import com.github.javafaker.Cat;
import com.github.javafaker.Commerce;
import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import com.google.common.collect.ImmutableList;
import dnl.utils.text.table.TextTable;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.SigV4WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.twilmes.sql.gremlin.SqlToGremlin;
import org.twilmes.sql.gremlin.processor.SingleQueryExecutor;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import software.amazon.jdbc.utilities.AuthScheme;
import software.amazon.jdbc.utilities.ConnectionProperties;
import software.amazon.neptune.gremlin.GremlinConnection;
import software.amazon.neptune.gremlin.GremlinConnectionProperties;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static software.amazon.neptune.gremlin.GremlinConnectionProperties.CONTACT_POINT_KEY;
import static software.amazon.neptune.gremlin.GremlinConnectionProperties.ENABLE_SSL_KEY;
import static software.amazon.neptune.gremlin.GremlinConnectionProperties.PORT_KEY;

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
    @Disabled
    void test() throws SQLException {
        final SqlToGremlin sqlToGremlin = new SqlToGremlin(null, getGraphTraversalSource());
        runQueryPrintResults("SELECT * FROM Person", sqlToGremlin);
        runQueryPrintResults("SELECT cat FROM Person", sqlToGremlin);
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

        final SchemaConfig schemaConfig =
                SqlGremlinQueryExecutor.getSqlGremlinGraphSchema(new GremlinConnectionProperties(properties));
        final SqlToGremlin sqlToGremlin = new SqlToGremlin(schemaConfig, getGraphTraversalSource());

        runQueryPrintResults("SELECT * FROM Person", sqlToGremlin);

        schemaConfig.getTables().forEach(table -> {
            final String tableName = table.getName();
            table.getColumns().forEach(column -> {
                try {
                    runQueryPrintResults(String.format("SELECT %s FROM %s", column.getName(), tableName), sqlToGremlin);
                } catch (final SQLException throwables) {
                    throwables.printStackTrace();
                }
            });
        });
    }

    void runQueryPrintResults(final String query, final SqlToGremlin sqlToGremlin) throws SQLException {
        System.out.println("Executing query: " + query);
        final SingleQueryExecutor.SqlGremlinQueryResult queryResult = sqlToGremlin.execute(query);
        final List<String> columns = queryResult.getColumns();
        final List<List<String>> rows = rowResultToString(queryResult);
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

    void runQueryPrintResults(final String query, final java.sql.Statement statement) throws SQLException {
        System.out.println("Executing query: " + query);
        final java.sql.ResultSet resultSet = statement.executeQuery(query);
        final int columnCount = resultSet.getMetaData().getColumnCount();
        final List<String> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            columns.add(resultSet.getMetaData().getColumnName(i));
        }

        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.println(resultSet.getString(i));
            }
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

    List<List<String>> rowResultToString(final SingleQueryExecutor.SqlGremlinQueryResult result) {
        final List<List<Object>> rows = result.getRows();
        final List<List<String>> stringRows = new ArrayList<>();
        for (final List<Object> row : rows) {
            final List<String> list = new ArrayList<>();
            for (final Object obj : row) {
                if (obj != null) {
                    if (obj instanceof Object[]) {
                        final Object[] object = (Object[]) obj;
                        for (final Object o : object) {
                            list.add(o == null ? null : o.toString());
                        }
                    } else {
                        list.add(obj.toString());
                    }
                } else {
                    list.add(null);
                }
            }
            stringRows.add(list);
        }
        return stringRows;
    }

    void printVertexes() {
        System.out.println("Input vertexes:");
        System.out.println("\t\tPerson: {\n" +
                "\t\t\t\"CAT\": \"Vincent\",\n" +
                "\t\t\t\"NAME\": \"LYNDON1\",\n" +
                "\t\t\t\"AGE\": 28\n\t\t}");
        System.out.println("\t\tPerson: {\n" +
                "\t\t\t\"DOG\": \"Ozwald\",\n" +
                "\t\t\t\"NAME\": \"LYNDON2\",\n" +
                "\t\t\t\"AGE\": \"28\"\n\t\t}");
        System.out.println("\t\tPerson: {\n" +
                "\t\t\t\"PETS\": \"[Vincent, Ozwald]\",\n" +
                "\t\t\t\"NAME\": \"LYNDON3\"\n\t\t}");
    }
}
