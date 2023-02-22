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

package software.aws.performance.implementations;

import software.aws.neptune.jdbc.utilities.AuthScheme;

public class PerformanceTestConstants {
    public static final String ENDPOINT = "no-auth-oc-enabled.cluster-cdubgfjknn5r.us-east-1.neptune.amazonaws.com";
    public static final String REGION = "us-east-1";
    public static final String SPARQL_ENDPOINT = "https://no-auth-oc-enabled.cluster-cdubgfjknn5r.us-east-1.neptune.amazonaws.com";
    public static final String SPARQL_QUERY = "sparql";
    public static final AuthScheme AUTH_SCHEME = AuthScheme.None;
    public static final int PORT = 8182;
    public static final int LIMIT_COUNT = 1000;

    public static final String SPARQL_ALL_DATA_QUERY = "SELECT ?s ?p ?o {?s ?p ?o}";
    public static final String SPARQL_ALL_DATA_LIMIT_QUERY = String.format("%s LIMIT %d", SPARQL_ALL_DATA_QUERY, LIMIT_COUNT);
    public static final String SPARQL_NUMBER_QUERY = "PREFIX prop:  <http://kelvinlawrence.net/air-routes/datatypeProperty/> " +
            "PREFIX class: <http://kelvinlawrence.net/air-routes/class/> " +
            "SELECT ?o " +
            "WHERE { " +
            "     ?s a class:Airport . " +
            "     ?s prop:elev ?o " +
            "} ";
    public static final String SPARQL_STRING_QUERY = "PREFIX prop:  <http://kelvinlawrence.net/air-routes/datatypeProperty/> " +
            "PREFIX class: <http://kelvinlawrence.net/air-routes/class/> " +
            "SELECT ?s ?o " +
            "WHERE { " +
            "    ?s a class:Airport . " +
            "    ?s prop:code ?o " +
            "} ";

    public static final String GREMLIN_ALL_DATA_QUERY = "g.V().valueMap().with(WithOptions.tokens)";
    public static final String GREMLIN_ALL_DATA_LIMIT_QUERY = String.format("%s.limit(%d)", GREMLIN_ALL_DATA_QUERY, LIMIT_COUNT);
    public static final String GREMLIN_NUMBER_QUERY = "g.V().hasLabel('airport').project('Elevation').by(values('elev'))";
    public static final String GREMLIN_STRING_QUERY = "g.V().hasLabel('airport').project('Elevation').by(values('code'))";

    public static final String OPENCYPHER_ALL_DATA_QUERY = "MATCH (n:airport) RETURN n";
    public static final String OPENCYPHER_ALL_DATA_LIMIT_QUERY = String.format("%s LIMIT %d", OPENCYPHER_ALL_DATA_QUERY, LIMIT_COUNT);
    public static final String OPENCYPHER_NUMBER_QUERY = "MATCH (a:airport) RETURN a.elev as Elevation";
    public static final String OPENCYPHER_STRING_QUERY = "MATCH (a:airport) RETURN a.code as Code";
}
