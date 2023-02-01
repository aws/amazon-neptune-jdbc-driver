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

package software.aws.neptune.gremlin.adapter.converter.ast.nodes;

import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.neptune.gremlin.adapter.converter.SqlMetadata;

/**
 * This abstract class in the GremlinSql equivalent of SqlNode.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 * @author Adapted from implementation by twilmes (https://github.com/twilmes/sql-gremlin)
 */
@AllArgsConstructor
public abstract class GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlNode.class);
    private final SqlNode sqlNode;
    private final SqlMetadata sqlMetadata;
}
