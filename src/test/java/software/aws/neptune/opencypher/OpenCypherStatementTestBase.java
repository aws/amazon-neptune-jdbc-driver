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

package software.aws.neptune.opencypher;

public class OpenCypherStatementTestBase {
    protected static final String QUICK_QUERY;
    protected static final String LONG_QUERY;
    protected static final int LONG_QUERY_NODE_COUNT = 1200;
    private static int currentIndex = 0;

    static {
        QUICK_QUERY = "CREATE (quick:Foo) RETURN quick";

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
