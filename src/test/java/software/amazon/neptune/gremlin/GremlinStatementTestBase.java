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
 *
 */

package software.amazon.neptune.gremlin;

import software.amazon.neptune.NeptuneStatementTestHelperBase;

public class GremlinStatementTestBase extends NeptuneStatementTestHelperBase {
    protected static final String QUICK_QUERY;
    protected static final int LONG_QUERY_NODE_COUNT = 1000;
    private static int currentIndex = 0;

    static {
        QUICK_QUERY = "1+1";
    }

    protected static String getLongQuery() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("g");
        for (int i = currentIndex; i < (currentIndex + LONG_QUERY_NODE_COUNT); i++) {
            stringBuilder.append(String.format(".addV('%d')", i));
        }
        currentIndex += LONG_QUERY_NODE_COUNT;
        return stringBuilder.toString();
    }
}
