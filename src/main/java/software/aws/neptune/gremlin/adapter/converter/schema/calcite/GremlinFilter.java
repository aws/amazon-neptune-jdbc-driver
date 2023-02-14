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

package software.aws.neptune.gremlin.adapter.converter.schema.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/**
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 * @author Adapted from implementation by twilmes (https://github.com/twilmes/sql-gremlin)
 *
 *
 */
public class GremlinFilter extends Filter implements GremlinRel {
    public GremlinFilter(
            final RelOptCluster cluster,
            final RelTraitSet traitSet,
            final RelNode child,
            final RexNode condition) {
        super(cluster, traitSet, child, condition);
    }

    @Override
    public Filter copy(final RelTraitSet traitSet, final RelNode input, final RexNode condition) {
        return new GremlinFilter(getCluster(), traitSet, input, condition);
    }
}
