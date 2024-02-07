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

package software.aws.neptune.common.gremlindatamodel;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.util.Pair;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.aws.neptune.gremlin.GremlinConnectionProperties;
import software.aws.neptune.gremlin.GremlinQueryExecutor;
import software.aws.neptune.gremlin.adapter.converter.schema.SqlSchemaGrabber;
import software.aws.neptune.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinEdgeTable;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinProperty;
import software.aws.neptune.gremlin.adapter.converter.schema.gremlin.GremlinVertexTable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MetadataCacheTest {
    private static final String ENDPOINT = "mockEndpoint";
    private static final String SERIALIZER = "GRAPHSON_V3D0";
    private final GremlinVertexTable testTableVertex;
    private final GremlinVertexTable testTableVertexBeta;
    private final GremlinEdgeTable testTableEdge;
    private final GremlinEdgeTable testTableEdgeBeta;
    private final GremlinSchema testFullSchema;

    MetadataCacheTest() {
        // Set up four tables, two vertex table with label "vertex" and "vertexBeta", and two edge tables with labels "edge", "edgeBeta".
        testTableVertex = new GremlinVertexTable("vertex",
                new ArrayList<>(Collections.singletonList(new GremlinProperty("testVertex", "string"))),
                new ArrayList<>(Collections.singletonList("edgeIn")),
                new ArrayList<>(Collections.singletonList("edgeOut")));
        testTableVertexBeta = new GremlinVertexTable("vertexBeta",
                new ArrayList<>(Collections.singletonList(new GremlinProperty("testVertex", "string"))),
                new ArrayList<>(Collections.singletonList("edgeIn")),
                new ArrayList<>(Collections.singletonList("edgeOut")));
        testTableEdge = new GremlinEdgeTable("edge",
                new ArrayList<>(Collections.singletonList(new GremlinProperty("testEdge", "string"))),
                new ArrayList<>(Collections.singletonList(new Pair<>("vertexIn", "vertexOut"))));
        testTableEdgeBeta = new GremlinEdgeTable("edgeBeta",
                new ArrayList<>(Collections.singletonList(new GremlinProperty("testEdge", "string"))),
                new ArrayList<>(Collections.singletonList(new Pair<>("vertexIn", "vertexOut"))));

        // Create a full schema to call the method on.
        testFullSchema = new GremlinSchema(new ArrayList<>(Arrays.asList(testTableVertex, testTableVertexBeta)),
                new ArrayList<>(Arrays.asList(testTableEdge, testTableEdgeBeta)));
    }

    @BeforeEach
    void refreshCache() {
        MetadataCache.getGremlinSchemas().clear();
    }

    @Test
    void testMockMetadataCache() throws SQLException {
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
        }
    }

    @Test
    void testMetadataCacheFiltering() throws SQLException {
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

    @Test
    void testUpdateGremlinCacheConnectionless() throws SQLException {
        GremlinConnectionProperties properties = new GremlinConnectionProperties();
        properties.setProperty(GremlinConnectionProperties.CONTACT_POINT_KEY, ENDPOINT);

        Assertions.assertFalse(MetadataCache.isMetadataCached(ENDPOINT));
        if (!MetadataCache.getGremlinSchemas().containsKey(ENDPOINT)) {
            MetadataCache.getGremlinSchemas().put(ENDPOINT, SqlSchemaGrabber.getSchema(TinkerGraph.open().traversal(), properties.getScanType()));
        }
        Assertions.assertTrue(MetadataCache.isMetadataCached(ENDPOINT));
    }

    @Test
    void testUpdateGremlinCacheAlreadyCached() throws SQLException {
        GremlinConnectionProperties properties = new GremlinConnectionProperties();
        properties.setProperty(GremlinConnectionProperties.CONTACT_POINT_KEY, ENDPOINT);

        MetadataCache.getGremlinSchemas().put(ENDPOINT, testFullSchema);
        Assertions.assertTrue(MetadataCache.isMetadataCached(ENDPOINT));

        MetadataCache.updateGremlinMetadataCache(properties);
        Assertions.assertEquals(testFullSchema, MetadataCache.getGremlinSchema(ENDPOINT));
    }

    @Test
    void testUpdateCacheIfNotUpdatedAlreadyCached() throws SQLException {
        GremlinConnectionProperties properties = new GremlinConnectionProperties();
        properties.setProperty(GremlinConnectionProperties.CONTACT_POINT_KEY, ENDPOINT);

        MetadataCache.getGremlinSchemas().put(ENDPOINT, testFullSchema);
        Assertions.assertTrue(MetadataCache.isMetadataCached(ENDPOINT));

        MetadataCache.updateCacheIfNotUpdated(properties);
        Assertions.assertEquals(testFullSchema, MetadataCache.getGremlinSchema(ENDPOINT));
    }

    @Test
    void testReceivingSerializerProperty() throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        GremlinConnectionProperties properties = new GremlinConnectionProperties();
        properties.setProperty(GremlinConnectionProperties.SERIALIZER_KEY, SERIALIZER);

        Method getClusterMethod = GremlinQueryExecutor.class.getDeclaredMethod("getCluster", GremlinConnectionProperties.class);
        getClusterMethod.setAccessible(true);
        Cluster cluster = (Cluster) getClusterMethod.invoke(null, properties);

        Method getSerializerMethod = Cluster.class.getDeclaredMethod("getSerializer");
        getSerializerMethod.setAccessible(true);
        Assertions.assertEquals(GraphSONMessageSerializerV3d0.class, getSerializerMethod.invoke(cluster).getClass());
    }
}
