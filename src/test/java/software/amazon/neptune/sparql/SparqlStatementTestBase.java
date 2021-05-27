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
 */

package software.amazon.neptune.sparql;

public class SparqlStatementTestBase {
    protected static final String QUICK_QUERY;
    protected static final String LONG_QUERY;
    protected static final int LONG_QUERY_NODE_COUNT = 500;
    private static int currentIndex = 0;

    static {
        QUICK_QUERY = "SELECT * { ?s ?p ?o } LIMIT 100";

        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = currentIndex; i < (currentIndex + LONG_QUERY_NODE_COUNT); i++) {
            stringBuilder.append(String.format("CREATE (node%d:Foo) ", i));
        }
        stringBuilder.append("RETURN ");
        for (int i = currentIndex; i < (currentIndex + LONG_QUERY_NODE_COUNT); i++) {
            if (i != currentIndex) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(String.format("node%d", i));
        }
        currentIndex += LONG_QUERY_NODE_COUNT;
        LONG_QUERY = stringBuilder.toString();
    }
}
