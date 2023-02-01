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

package software.aws.neptune.gremlin.adapter.converter.schema.calcite;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinEdgeTable;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinTableBase;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinVertexTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by twilmes on 9/22/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
@AllArgsConstructor
public class GremlinSchema extends AbstractSchema {
    private final List<GremlinVertexTable> vertices;
    private final List<GremlinEdgeTable> edges;

    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        builder.putAll(vertices.stream().collect(Collectors.toMap(GremlinTableBase::getLabel, t -> t)));
        builder.putAll(edges.stream().collect(Collectors.toMap(GremlinTableBase::getLabel, t -> t)));
        final Map<String, Table> tableMap = builder.build();
        return tableMap;
    }

    public List<GremlinVertexTable> getVertices() {
        return new ArrayList<>(vertices);
    }

    public List<GremlinEdgeTable> getEdges() {
        return new ArrayList<>(edges);
    }

    public List<GremlinTableBase> getAllTables() {
        final List<GremlinTableBase> gremlinTableBases = new ArrayList<>();
        gremlinTableBases.addAll(vertices);
        gremlinTableBases.addAll(edges);
        return gremlinTableBases;
    }
}
