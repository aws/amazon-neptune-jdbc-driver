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

import lombok.SneakyThrows;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import software.aws.neptune.gremlin.adapter.util.SqlGremlinError;

/**
 * List of rules that get pushed down and converted into GremlinTraversals.  Right now
 * only filter is pushed down using rules.  Joins are converted, but handled the by RelWalker
 * utilities.
 * <p>
 * Created by twilmes on 11/14/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
class GremlinRules {
    public static final RelOptRule[] RULES = {
            GremlinFilterRule.INSTANCE
    };

    abstract static class GremlinConverterRule extends ConverterRule {
        private final Convention out;

        GremlinConverterRule(
                final Class<? extends RelNode> clazz,
                final RelTrait in,
                final Convention out,
                final String description) {
            super(clazz, in, out, description);
            this.out = out;
        }

        protected Convention getOut() {
            return out;
        }
    }

    private static final class GremlinFilterRule extends GremlinConverterRule {
        private static final GremlinFilterRule INSTANCE = new GremlinFilterRule();

        private GremlinFilterRule() {
            super(LogicalFilter.class, Convention.NONE, GremlinRel.CONVENTION, "GremlinFilterRule");
        }

        @SneakyThrows
        public RelNode convert(final RelNode rel) {
            if (!(rel instanceof LogicalFilter)) {
                throw SqlGremlinError.create(SqlGremlinError.NOT_LOGICAL_FILTER, rel.getClass().getName(),
                        LogicalFilter.class.getName());
            }
            final LogicalFilter filter = (LogicalFilter) rel;
            final RelTraitSet traitSet = filter.getTraitSet().replace(getOut());
            return new GremlinFilter(rel.getCluster(), traitSet, convert(filter.getInput(), getOut()),
                    filter.getCondition());
        }
    }

}
