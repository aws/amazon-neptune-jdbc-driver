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

package software.aws.neptune.opencypher.mock;

public class OpenCypherQueryLiterals {
    // Primitive
    public static final String NULL = "RETURN null as n";
    public static final String POS_INTEGER = "RETURN 1 AS n";
    public static final String NEG_INTEGER = "RETURN -1 AS n";
    public static final String ZERO_INTEGER = "RETURN 0 AS n";
    public static final String NON_EMPTY_STRING = "RETURN 'hello' AS n";
    public static final String EMPTY_STRING = "RETURN '' AS n";
    public static final String TRUE = "RETURN true AS n";
    public static final String FALSE = "RETURN false AS n";
    public static final String POS_DOUBLE = "RETURN 1.0 AS n";
    public static final String NEG_DOUBLE = "RETURN -1.0 AS n";
    public static final String ZERO_DOUBLE = "RETURN 0.0 AS n";

    // Composite
    public static final String EMPTY_MAP = "RETURN ({}) AS n";
    public static final String NON_EMPTY_MAP = "RETURN ({hello:'world'}) AS n";
    public static final String EMPTY_LIST = "RETURN [] AS n";
    public static final String NON_EMPTY_LIST = "RETURN ['hello', 'world'] AS n";

    // Date, time, point
    public static final String DATE = "RETURN date(\"1993-03-30\") AS n";
    public static final String TIME = "RETURN time(\"12:10:10.225+0100\") AS n";
    public static final String LOCAL_TIME = "RETURN localtime(\"12:10:10.225\") AS n";
    public static final String DATE_TIME = "RETURN datetime(\"1993-03-30T12:10:10.225+0100\") AS n";
    public static final String LOCAL_DATE_TIME = "RETURN localdatetime(\"1993-03-30T12:10:10.225\") AS n";
    public static final String DURATION = "RETURN duration(\"P5M1.5D\") as n";
    public static final String POINT_2D = "RETURN point({ x:0, y:1 }) AS n";
    public static final String POINT_3D = "RETURN point({ x:0, y:1, z:2 }) AS n";

    // Graph
    public static final String NODE = "CREATE (node:Foo) RETURN node";
    public static final String RELATIONSHIP = "CREATE (node1:Foo)-[rel:Rel]->(node2:Bar) RETURN rel";
    public static final String PATH = "CREATE (node1:Foo)-[rel:Rel]->(node2:Bar) " +
            "CREATE (node2)-[rel2:Rel]->(node3:Baz) " +
            "WITH (node1)-[:Rel*]->(node3) AS R " +
            "MATCH p = (node1)-[:Rel*]->(node3) " +
            "RETURN p";
}
