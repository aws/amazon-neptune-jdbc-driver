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

/**
 * Created by twilmes on 9/25/15.
 * Modified by lyndonb-bq on 05/17/21.
 */

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rule to convert a relational expression from
 * {@link GremlinRel#CONVENTION} to {@link EnumerableConvention}.
 */
public final class GremlinToEnumerableConverterRule extends ConverterRule {
    public static final ConverterRule INSTANCE =
            new GremlinToEnumerableConverterRule();

    private GremlinToEnumerableConverterRule() {
        super(RelNode.class, GremlinRel.CONVENTION, EnumerableConvention.INSTANCE,"GremlinToEnumerableConverterRule");
    }

    @Override
    public RelNode convert(final RelNode rel) {
        final RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
        return new GremlinToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
}
