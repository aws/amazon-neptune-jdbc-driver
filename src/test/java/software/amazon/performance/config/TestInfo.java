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

package software.amazon.performance.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TestInfo {
    private static final String JDBC = "jdbc:";

    @JsonProperty("description")
    private String description;
    public String getDescription() {
        return description;
    }

    @JsonProperty("connection_type")
    private String connectionType;
    public String getConnectionType() {
        return connectionType;
    }

    @JsonProperty("server")
    private String server;
    public String getServer() {
        return server;
    }

    @JsonProperty("repeats")
    private int repeats;
    public int getRepeats() {
        return repeats;
    }

    @JsonProperty("concurrency_config")
    private ConcurrencyTestInfo concurrencyTestInfo;
    public ConcurrencyTestInfo getConcurrencyTestInfo() {
        return concurrencyTestInfo;
    }

    @JsonProperty("latency_config")
    private LatencyTestInfo latencyTestInfo;
    public LatencyTestInfo getLatencyTestInfo() {
        return latencyTestInfo;
    }

    @JsonProperty("load_config")
    private LoadTestInfo loadTestInfo;
    public LoadTestInfo getLoadTestInfo() {
        return loadTestInfo;
    }

    /**
     * Gets url based on the connection type and server info.
     * @return Url based on the connection type and server info.
     */
    public String url() {
        return JDBC + connectionType + "://" + server;
    }
}
