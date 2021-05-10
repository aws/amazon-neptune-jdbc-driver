/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 */

package sample.applications;

public class ExampleConfigs {
    private static final String JDBC_OPENCYPHER_CONNECTION_STRING = "jdbc:neptune:opencypher://bolt://%s:%d";
    private static final String URL = "example-url.com";
    private static final int PORT = 8182;

    public static String getConnectionString() {
        return String.format(JDBC_OPENCYPHER_CONNECTION_STRING, URL, PORT);
    }
}
