/*
 * Copyright <2022> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.aws.neptune.common.gremlindatamodel;

import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.aws.neptune.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinEdgeTable;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinProperty;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinVertexTable;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MetadataCacheTest {
    private static final String ENDPOINT = "mockEndpoint";

    @Test
    void testMetadataCacheFiltering() throws SQLException {
        // Set up four tables, two vertex table with label "vertex" and "vertexBeta", and two edge tables with labels "edge", "edgeBeta".
        final GremlinVertexTable testTableVertex = new GremlinVertexTable("vertex",
                new ArrayList<>(Collections.singletonList(new GremlinProperty("testVertex", "string"))),
                new ArrayList<>(Collections.singletonList("edgeIn")),
                new ArrayList<>(Collections.singletonList("edgeOut")));
        final GremlinVertexTable testTableVertexBeta = new GremlinVertexTable("vertexBeta",
                new ArrayList<>(Collections.singletonList(new GremlinProperty("testVertex", "string"))),
                new ArrayList<>(Collections.singletonList("edgeIn")),
                new ArrayList<>(Collections.singletonList("edgeOut")));
        final GremlinEdgeTable testTableEdge = new GremlinEdgeTable("edge",
                new ArrayList<>(Collections.singletonList(new GremlinProperty("testEdge", "string"))),
                new ArrayList<>(Collections.singletonList(new Pair<>("vertexIn", "vertexOut"))));
        final GremlinEdgeTable testTableEdgeBeta = new GremlinEdgeTable("edgeBeta",
                new ArrayList<>(Collections.singletonList(new GremlinProperty("testEdge", "string"))),
                new ArrayList<>(Collections.singletonList(new Pair<>("vertexIn", "vertexOut"))));

        // Create a full schema to call the method on.
        final GremlinSchema testFullSchema = new GremlinSchema(new ArrayList<>(Arrays.asList(testTableVertex, testTableVertexBeta)),
                new ArrayList<>(Arrays.asList(testTableEdge, testTableEdgeBeta)));

        try (MockedStatic<MetadataCache> mockMetadataCache = Mockito.mockStatic(MetadataCache.class, invocation -> {
            final Method method = invocation.getMethod();
            if ("getGremlinSchemas".equals(method.getName())) {
                return invocation.getMock();
            } else {
                return invocation.callRealMethod();
            }
        })) {
            final Map<String, GremlinSchema> schemaMap = new HashMap<>();
            schemaMap.put(ENDPOINT, testFullSchema);
            mockMetadataCache.when(MetadataCache::getGremlinSchemas).thenReturn(schemaMap);
            Assertions.assertEquals(schemaMap, MetadataCache.getGremlinSchemas());

            // Assert that filtering based label only gets the specified table.
            final GremlinSchema generatedVertexSchema = MetadataCache.getFilteredCacheNodeColumnInfos("vertex", ENDPOINT);
            Assertions.assertEquals("vertex", generatedVertexSchema.getVertices().get(0).getLabel());
            Assertions.assertEquals(testTableVertex, generatedVertexSchema.getVertices().get(0));
            Assertions.assertEquals(1, generatedVertexSchema.getVertices().size());
            Assertions.assertEquals(0, generatedVertexSchema.getEdges().size());

            final GremlinSchema generatedVertexBetaSchema = MetadataCache.getFilteredCacheNodeColumnInfos("vertexBeta", ENDPOINT);
            Assertions.assertEquals("vertexBeta", generatedVertexBetaSchema.getVertices().get(0).getLabel());
            Assertions.assertEquals(testTableVertexBeta, generatedVertexBetaSchema.getVertices().get(0));
            Assertions.assertEquals(1, generatedVertexBetaSchema.getVertices().size());
            Assertions.assertEquals(0, generatedVertexBetaSchema.getEdges().size());

            final GremlinSchema generatedEdgeSchema = MetadataCache.getFilteredCacheNodeColumnInfos("edge", ENDPOINT);
            Assertions.assertEquals("edge", generatedEdgeSchema.getEdges().get(0).getLabel());
            Assertions.assertEquals(testTableEdge, generatedEdgeSchema.getEdges().get(0));
            Assertions.assertEquals(0, generatedEdgeSchema.getVertices().size());
            Assertions.assertEquals(1, generatedEdgeSchema.getEdges().size());

            final GremlinSchema generatedEdgeBetaSchema = MetadataCache.getFilteredCacheNodeColumnInfos("edgeBeta", ENDPOINT);
            Assertions.assertEquals("edgeBeta", generatedEdgeBetaSchema.getEdges().get(0).getLabel());
            Assertions.assertEquals(testTableEdgeBeta, generatedEdgeBetaSchema.getEdges().get(0));
            Assertions.assertEquals(0, generatedEdgeBetaSchema.getVertices().size());
            Assertions.assertEquals(1, generatedEdgeBetaSchema.getEdges().size());
        }
    }
}
