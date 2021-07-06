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

package software.amazon.neptune.sparql;

public class SparqlStatementTestBase {
    protected static final String QUICK_QUERY;
    protected static final String LONG_QUERY;
    public static final String LONG_UPDATE;
    public static final String LONG_UPDATE_TWO;
    protected static final int LONG_UPDATE_COUNT = 500;
    private static int currentIndex = 0;
    private static int currentIndexTwo = 0;

    static {
        QUICK_QUERY = "SELECT * { ?s ?p ?o } LIMIT 10";
        //LONG_QUERY = "SELECT ?s ?p ?o WHERE { ?s ?p ?o FILTER(!ISNUMERIC(?o)) }";
        LONG_QUERY = "SELECT ?s ?p ?o WHERE { " +
                "{ SELECT * WHERE { ?s ?p ?o FILTER(CONTAINS(LCASE(?o), \"string\")) } " +
                "}" +
                "UNION " +
                "{ SELECT * WHERE { ?s ?p ?o FILTER(!ISNUMERIC(?o)) } " +
                "} " +
                "}";

        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("PREFIX : <http://example/> ");
        for (int i = currentIndex; i < (currentIndex + LONG_UPDATE_COUNT); i++) {
            stringBuilder.append(String.format("INSERT DATA { :s :p \"string%d\" };", i));
        }
        currentIndex += LONG_UPDATE_COUNT;
        LONG_UPDATE = stringBuilder.toString();

        final StringBuilder stringBuilder2 = new StringBuilder();
        stringBuilder2.append("PREFIX : <http://example/> ");
        for (int i = currentIndexTwo; i < (currentIndexTwo + LONG_UPDATE_COUNT); i++) {
            stringBuilder2.append(String.format("INSERT DATA { :s :p \"string%d\" };", i));
        }
        currentIndexTwo += LONG_UPDATE_COUNT;
        LONG_UPDATE_TWO = stringBuilder2.toString();
    }
}
