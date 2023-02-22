/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.sparql;

public class SparqlStatementTestBase {
    protected static final String QUICK_QUERY;
    protected static final String LONG_QUERY;
    public static final String LONG_UPDATE;
    protected static final int LONG_UPDATE_COUNT = 500;
    private static int currentIndex = 0;

    static {
        QUICK_QUERY = "SELECT * { ?s ?p ?o } LIMIT 10";
        LONG_QUERY = "SELECT ?s ?p ?o WHERE { " +
                "{ SELECT * WHERE { ?s ?p ?o FILTER(CONTAINS(LCASE(?o), \"string\")) } " +
                "} " +
                "UNION " +
                "{ SELECT * WHERE { ?s ?p ?o FILTER(!ISNUMERIC(?o)) } " +
                "} " +
                "UNION " +
                "{ SELECT * WHERE { ?s ?p ?o FILTER(CONTAINS(LCASE(?s), \"example\")) } " +
                "} " +
                "}";

        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("PREFIX : <http://example/> ");
        for (int i = currentIndex; i < (currentIndex + LONG_UPDATE_COUNT); i++) {
            stringBuilder.append(String.format("INSERT DATA { :s :p \"string%d\" };", i));
        }
        currentIndex += LONG_UPDATE_COUNT;
        LONG_UPDATE = stringBuilder.toString();
    }
}
