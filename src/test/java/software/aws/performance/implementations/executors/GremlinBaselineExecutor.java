/*
 * Copyright <2023> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.neptune.gremlin.GremlinQueryExecutor;
import software.aws.performance.PerformanceTestExecutor;
import software.aws.performance.implementations.PerformanceTestConstants;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static software.aws.neptune.gremlin.GremlinConnectionProperties.CONTACT_POINT_KEY;
import static software.aws.neptune.gremlin.GremlinConnectionProperties.PORT_KEY;
import static software.aws.neptune.jdbc.utilities.ConnectionProperties.AUTH_SCHEME_KEY;

public class GremlinBaselineExecutor extends PerformanceTestExecutor {
    private final Client client;

    /**
     * Constructor for GremlinBaselineExecutor.
     */
    @SneakyThrows
    public GremlinBaselineExecutor() {
        final Properties properties = new Properties();
        properties.put(CONTACT_POINT_KEY, PerformanceTestConstants.ENDPOINT);
        properties.put(PORT_KEY, PerformanceTestConstants.PORT);
        properties.put(AUTH_SCHEME_KEY, PerformanceTestConstants.AUTH_SCHEME);
        final GremlinConnectionProperties gremlinConnectionProperties = new GremlinConnectionProperties(properties);
        final Cluster cluster = GremlinQueryExecutor.createClusterBuilder(gremlinConnectionProperties).create();
        client = cluster.connect();
        client.init();
    }


    @Override
    @SneakyThrows
    protected Object execute(final String query) {
        return client.submit(query).all().get();
    }

    @Override
    @SneakyThrows
    protected int retrieve(final Object retrieveObject) {
        if (!(retrieveObject instanceof List<?>)) {
            throw new Exception("Error: expected a List<?> for data retrieval.");
        }
        int rowCount = 0;
        final List<Result> results = (List<Result>) retrieveObject;
        for (final Result r : results) {
            rowCount++;
            if (!(r.getObject() instanceof LinkedHashMap)) {
                // Best way to handle it seems to be to issue a warning.
                throw new Exception("Error, data has unexpected format.");
            }
            final Map<?, ?> row = (LinkedHashMap<?, ?>) r.getObject();
            final Set<?> keys = row.keySet();
            for (final Object key : keys) {
                row.get(key);
            }
        }
        return rowCount;
    }

    @Override
    @SneakyThrows
    protected int retrieveString(final Object retrieveObject) {
        if (!(retrieveObject instanceof List<?>)) {
            throw new Exception("Error: expected a List<?> for data retrieval.");
        }
        int rowCount = 0;
        final List<Result> results = (List<Result>) retrieveObject;
        for (final Result r : results) {
            rowCount++;
            if (!(r.getObject() instanceof LinkedHashMap)) {
                // Best way to handle it seems to be to issue a warning.
                throw new Exception("Error, data has unexpected format.");
            }
            final Map<?, ?> row = (LinkedHashMap<?, ?>) r.getObject();
            final Set<?> keys = row.keySet();
            for (final Object key : keys) {
                Object data = row.get(key);
                if (!(row.get(key) instanceof String)) {
                    data = data.toString();
                }
            }
        }
        return rowCount;
    }

    @Override
    @SneakyThrows
    protected int retrieveInteger(final Object retrieveObject) {
        if (!(retrieveObject instanceof List<?>)) {
            throw new Exception("Error: expected a List<?> for data retrieval.");
        }
        int rowCount = 0;
        final List<Result> results = (List<Result>) retrieveObject;
        for (final Result r : results) {
            rowCount++;
            if (!(r.getObject() instanceof LinkedHashMap)) {
                // Best way to handle it seems to be to issue a warning.
                throw new Exception("Error, data has unexpected format.");
            }
            final Map<?, ?> row = (LinkedHashMap<?, ?>) r.getObject();
            final Set<?> keys = row.keySet();
            for (final Object key : keys) {
                Object data = row.get(key);
                if (!(row.get(key) instanceof Number)) {
                    data = Integer.parseInt(data.toString());
                }
            }
        }
        return rowCount;
    }
}
