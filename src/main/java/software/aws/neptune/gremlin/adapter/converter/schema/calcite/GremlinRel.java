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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Created by twilmes on 9/25/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public interface GremlinRel extends RelNode {
    /**
     * Calling convention for relational operations that occur in Gremlin.
     */
    Convention CONVENTION = new Convention.Impl("GREMLIN", GremlinRel.class);
}
