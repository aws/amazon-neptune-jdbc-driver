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

package software.amazon.neptune.opencypher;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OpenCypherManualIAMTest {
    private static final int PORT = 8182;
    private static final String HOSTNAME = "iam-auth-test.cluster-cdubgfjknn5r.us-east-1.neptune.amazonaws.com";
    private static final String ENDPOINT = String.format("bolt://%s:%d", HOSTNAME, PORT);
    private static final String REGION = "us-east-1";
    private static final String AUTH = "IamSigV4";
    private static final String ENCRYPTION = "TRUE";
    private static final String CONNECTION_STRING =
            String.format("jdbc:neptune:opencypher://%s;useEncryption=%s;authScheme=%s;region=%s;", ENDPOINT,
                    ENCRYPTION,
                    AUTH, REGION);
    private static final String CREATE_NODES = String.format("CREATE (:%s %s)", "Person:Developer", "{hello:'world'}") +
            String.format(" CREATE (:%s %s)", "Person", "{person:1234}") +
            String.format(" CREATE (:%s %s)", "Human", "{hello:'world'}") +
            String.format(" CREATE (:%s %s)", "Human", "{hello:123}") +
            String.format(" CREATE (:%s %s)", "Developer", "{hello:123}") +
            String.format(" CREATE (:%s %s)", "Person", "{p1:true}") +
            String.format(" CREATE (:%s %s)", "Person", "{p1:1.0}") +
            " CREATE (:Foo {foo:'foo'})-[:Rel {rel:'rel'}]->(:Bar {bar:'bar'})";
    private final Map<String, List<Map.Entry<String, String>>> tableMap = new HashMap<>();

    @Disabled
    @Test
    void testBasicIamAuth() throws Exception {
        final Connection connection = DriverManager.getConnection(CONNECTION_STRING);
        Assertions.assertTrue(connection.isValid(1));
    }

    @Disabled
    @Test
    void testLongRunningQuery() throws Exception {
        final Properties properties = new Properties();
        properties.put(OpenCypherConnectionProperties.ENDPOINT_KEY, ENDPOINT);
        properties.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, AUTH);
        properties.put(OpenCypherConnectionProperties.REGION_KEY, REGION);
        final Connection connection = new OpenCypherConnection(new OpenCypherConnectionProperties(properties));
        Assertions.assertTrue(connection.isValid(1));
        final Statement statement = connection.createStatement();
        final Instant start = Instant.now();
        try {
            launchCancelThread(statement, 1000);
            final ResultSet resultSet = statement.executeQuery(createLongQuery());
        } catch (final SQLException e) {
            System.out.println("Encountered exception: " + e);
        }
        final Instant end = Instant.now();
        System.out.println("Time diff: " + Duration.between(start, end).toMillis() + " ms");
    }

    @Disabled
    @Test
    void testGetColumns() throws SQLException {
        final Connection connection = DriverManager.getConnection(CONNECTION_STRING);
        final DatabaseMetaData databaseMetaData = connection.getMetaData();
        final ResultSet resultSet = databaseMetaData.getColumns(null, null, null, null);
        Assertions.assertTrue(resultSet.next());
        do {
            final String table = resultSet.getString("TABLE_NAME");
            final String column = resultSet.getString("COLUMN_NAME");
            final String type = resultSet.getString("TYPE_NAME");
            if (!tableMap.containsKey(table)) {
                tableMap.put(table, new ArrayList<>());
            }
            tableMap.get(table).add(new AbstractMap.SimpleImmutableEntry<String, String>(column, type) {
            });
        } while (resultSet.next());

        for (final Map.Entry<String, List<Map.Entry<String, String>>> entry : tableMap.entrySet()) {
            System.out.println("Table: " + entry.getKey());
            for (final Map.Entry<String, String> columnTypePair : entry.getValue()) {
                System.out.println("\tColumn: " + columnTypePair.getKey() + ",\t Type: " + columnTypePair.getValue());
            }
        }
    }

    @Disabled
    @Test
    void testGetTables() throws SQLException {
        final Connection connection = DriverManager.getConnection(CONNECTION_STRING);
        final DatabaseMetaData databaseMetaData = connection.getMetaData();
        final ResultSet resultSet = databaseMetaData.getTables(null, null, null, null);
        Assertions.assertTrue(resultSet.next());
        do {
            final int columnCount = resultSet.getMetaData().getColumnCount();
            System.out.println("Table: " + resultSet.getString("TABLE_NAME"));
            for (int i = 0; i < columnCount; i++) {
                final String columnName = resultSet.getMetaData().getColumnName(i + 1);
                if (!"TABLE_NAME".equals(columnName)) {
                    System.out.println("\t" + resultSet.getMetaData().getColumnName(i + 1) + " - '" +
                            resultSet.getString(i + 1) + "'");
                }
            }
        } while (resultSet.next());
    }

    String createLongQuery() {
        final int nodeCount = 1000;
        final StringBuilder createStatement = new StringBuilder();
        for (int i = 0; i < nodeCount; i++) {
            createStatement.append(String.format("CREATE (node%d:Foo)", i));
            if (i != (nodeCount - 1)) {
                createStatement.append(" ");
            }
        }
        return createStatement.toString();
    }

    void launchCancelThread(final Statement statement, final int waitTime) {
        final ExecutorService cancelThread = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("cancelThread").setDaemon(true).build());
        cancelThread.execute(new Cancel(statement, waitTime));
    }


    /**
     * Class to cancel query in a separate thread.
     */
    @AllArgsConstructor
    public static class Cancel implements Runnable {
        private final Statement statement;
        private final int waitTime;

        @SneakyThrows
        @Override
        public void run() {
            try {
                Thread.sleep(waitTime);
                statement.cancel();
            } catch (final SQLException e) {
                System.out.println("Cancel exception: " + e);
            }
        }
    }
}
