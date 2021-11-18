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

package software.aws.performance.implementations.executors;

import lombok.SneakyThrows;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import software.aws.neptune.jdbc.utilities.AuthScheme;
import software.aws.neptune.opencypher.OpenCypherConnectionProperties;
import software.aws.neptune.opencypher.OpenCypherIAMRequestGenerator;
import software.aws.performance.PerformanceTestExecutor;
import software.aws.performance.implementations.PerformanceTestConstants;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class OpenCypherBaselineExecutor extends PerformanceTestExecutor {
    private final Session session;

    /**
     * Constructor for OpenCypherBaselineExecutor.
     */
    @SneakyThrows
    public OpenCypherBaselineExecutor() {
        final Properties properties = new Properties();
        properties.put(OpenCypherConnectionProperties.ENDPOINT_KEY,
                String.format("bolt://%s:%d", PerformanceTestConstants.ENDPOINT, PerformanceTestConstants.PORT));
        properties.put(OpenCypherConnectionProperties.AUTH_SCHEME_KEY, PerformanceTestConstants.AUTH_SCHEME);
        properties.put(OpenCypherConnectionProperties.SERVICE_REGION_KEY, PerformanceTestConstants.REGION);
        final OpenCypherConnectionProperties openCypherConnectionProperties =
                new OpenCypherConnectionProperties(properties);

        final Config.ConfigBuilder configBuilder = Config.builder();
        final boolean useEncryption = openCypherConnectionProperties.getUseEncryption();
        if (useEncryption) {
            configBuilder.withEncryption();
            configBuilder.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        } else {
            configBuilder.withoutEncryption();
        }
        configBuilder.withMaxConnectionPoolSize(openCypherConnectionProperties.getConnectionPoolSize());
        configBuilder
                .withConnectionTimeout(openCypherConnectionProperties.getConnectionTimeoutMillis(),
                        TimeUnit.MILLISECONDS);

        AuthToken authToken = AuthTokens.none();
        if (openCypherConnectionProperties.getAuthScheme().equals(AuthScheme.IAMSigV4)) {
            authToken = OpenCypherIAMRequestGenerator
                    .getSignedHeader(openCypherConnectionProperties.getEndpoint(),
                            openCypherConnectionProperties.getRegion());
        }
        final Driver driver = GraphDatabase.driver(openCypherConnectionProperties.getEndpoint(), authToken,
                configBuilder.build());
        session = driver.session();
    }

    @Override
    protected Object execute(final String query) {
        return session.run(query).list();
    }

    @Override
    @SneakyThrows
    protected int retrieve(final Object retrieveObject) {
        if (!(retrieveObject instanceof List)) {
            throw new Exception("Error: expected a Result for data retrieval.");
        }

        int rowCount = 0;
        final List<Record> recordList = (List<Record>) retrieveObject;
        for (final Record r : recordList) {
            final List<Value> values = r.values();
            for (int i = 0; i < values.size(); i++) {
                values.get(i);
            }
            rowCount++;
        }
        return rowCount;
    }

    @Override
    @SneakyThrows
    protected int retrieveString(final Object retrieveObject) {
        if (!(retrieveObject instanceof List)) {
            throw new Exception("Error: expected a Result for data retrieval.");
        }

        int rowCount = 0;
        final List<Record> recordList = (List<Record>) retrieveObject;
        for (final Record r : recordList) {
            final List<Value> values = r.values();
            for (int i = 0; i < values.size(); i++) {
                r.get(i).asString();
            }
            rowCount++;
        }
        return rowCount;
    }

    @Override
    @SneakyThrows
    protected int retrieveInteger(final Object retrieveObject) {
        if (!(retrieveObject instanceof List)) {
            throw new Exception("Error: expected a Result for data retrieval.");
        }

        int rowCount = 0;
        final List<Record> recordList = (List<Record>) retrieveObject;
        for (final Record r : recordList) {
            final List<Value> values = r.values();
            for (int i = 0; i < values.size(); i++) {
                r.get(i).asInt();
            }
            rowCount++;
        }
        return rowCount;
    }
}
