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

package software.aws.neptune.gremlin.adapter.graphs;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import software.aws.neptune.gremlin.adapter.GremlinSqlBaseTest;

public class TestGraphFactory {
    private static final TestGraph SPACE_GRAPH = new SpaceTestGraph();
    private static final TestGraph SPACE_INCOMPLETE_GRAPH = new SpaceTestIncompleteGraph();
    private static final TestGraph DATA_TYPE_GRAPH = new DataTypeGraph();

    public static Graph createGraph(final GremlinSqlBaseTest.DataSet dataSet) {
        final Graph graph = TinkerGraph.open();
        switch (dataSet) {
            case SPACE:
                SPACE_GRAPH.populate(graph);
                break;
            case SPACE_INCOMPLETE:
                SPACE_INCOMPLETE_GRAPH.populate(graph);
                break;
            case DATA_TYPES:
                DATA_TYPE_GRAPH.populate(graph);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported graph " + dataSet.name());
        }
        return graph;
    }
}
